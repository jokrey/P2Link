package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.*;

/**
 * IDEA:
 *   a new node does only need an internet connection and know the link of a single node with a public link
 *   using only that it can become a node to which other nodes can connect
 *
 * Types of connection establishing:
 * direct - the answerer node has a public link and can be directly accessed
 */
class EstablishSingleConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    //todo: mtu detection + exchange
    //  todo sending and requesting self proclaimed public link(NOT CHECKED FOR DDOS REASONS)
    static boolean asInitiator(P2LNodeInternal parent, P2Link to) throws IOException {
        return asInitiator(parent, to, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }
    static boolean asInitiator(P2LNodeInternal parent, P2Link to, int attempts, int initialTimeout) throws IOException {
        //todo - RE-establish connection protocol that does not do the nonce check - for historic connections (more efficient retry)

        boolean success = parent.tryReceive(attempts, initialTimeout, () -> {
            if (parent.isConnectedTo(to)) return new P2LFuture<>(false);
            int conversationId = parent.createUniqueConversationId();
            return parent.expectInternalMessage(to.getSocketAddress(), R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId)
                    //todo - this syntax is kinda confusing - the message is obviously send before it is received (but writing it sequentially would cause a potential race condition with instantly expiring message, which are beneficial in retried protocols)
                    .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PEER_CONNECTION_REQUEST, conversationId), to.getSocketAddress()))
                    .combine(message -> {
                        byte[] verifyNonce = message.asBytes();
                        if (verifyNonce.length == 0) return new P2LFuture<>(false);
                        if (verifyNonce.length == 1) {
                            return new P2LFuture<>(true);
                        }
                        try {
                            return parent.expectInternalMessage(to.getSocketAddress(), R_CONNECTION_ESTABLISHED, conversationId).toBooleanFuture(m -> true)
                                    .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId, P2LMessage.EXPIRE_INSTANTLY, verifyNonce), to.getSocketAddress()));
                        } catch (Throwable e) {
                            e.printStackTrace();
                            return new P2LFuture<>(null);//causes retry!!
                        }
                    });
        });

        if (success)
            parent.graduateToEstablishedConnection(to);
        return success;
    }

    static void asAnswerer(P2LNodeInternal parent, SocketAddress from, P2LMessage initialRequestMessage) throws Throwable {
        //todo receive self proclaimed peer link from peer OR create hidden link from 'from'
//        P2Link naiveLinkOfPeer = P2Link.createHiddenLink(parent.getSelfLink(), (InetSocketAddress) from);
        P2Link naiveLinkOfPeer = P2Link.raw(from); //todo this is not correct

        int conversationId = initialRequestMessage.header.getConversationId();
        if (parent.connectionLimitReached()) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY), from); //do not retry refusal
            return;
        }
        if (parent.isConnectedTo(naiveLinkOfPeer)) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY, new byte[1]), from); //do not retry refusal
            return;
        }


        // send a nonce to other peer
        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
        byte[] nonce = new byte[16];
        secureRandom.nextBytes(nonce);
        //next line blocking, due to the following problem: if the packet is lost, the initiator will retry the connection:
        //     sending a new connection-request + receiving a verify nonce request WITH A DIFFERENT NONCE - however, since this came first, we will receive the nonce here...

        byte[] verifyNonce = parent.expectInternalMessage(from, R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId)
                .nowOrCancel(() ->
                        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY, nonce), from)
                )
                .get(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT).asBytes();
        if (Arrays.equals(nonce, verifyNonce)) {
            parent.graduateToEstablishedConnection(naiveLinkOfPeer);
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_ESTABLISHED, conversationId, P2LMessage.EXPIRE_INSTANTLY), from);
        }
    }
}