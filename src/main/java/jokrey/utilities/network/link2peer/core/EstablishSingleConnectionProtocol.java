package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class EstablishSingleConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    static boolean asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        return asInitiator(parent, to, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }
    static boolean asInitiator(P2LNodeInternal parent, SocketAddress to, int attempts, int initialTimeout) throws IOException {
//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.asInitiator(parent, link, outgoing);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }

        //todo - REestablish connection protocol that does not do the nonce check - for historic connections (more efficient retry)

        boolean success = parent.tryReceive(attempts, initialTimeout, () -> {
            if (parent.isConnectedTo(to)) return new P2LFuture<>(false);
            int conversationId = parent.createUniqueConversationId();
            return parent.expectInternalMessage(to, R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId)
                    //todo - this syntax is kinda confusing - the message is obviously send before it is received (but writing it sequentially would cause a potential race condition with instantly expiring message, which are beneficial in retried protocols)
                    .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PEER_CONNECTION_REQUEST, conversationId), to))
                    .combine(message -> {
                        byte[] verifyNonce = message.asBytes();
                        if (verifyNonce.length == 0) return new P2LFuture<>(false);
                        if (verifyNonce.length == 1) {
                            return new P2LFuture<>(true);
                        }
                        try {
                            return parent.expectInternalMessage(to, R_CONNECTION_ESTABLISHED, conversationId).toBooleanFuture(m -> true)
                                    .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId, P2LMessage.EXPIRE_INSTANTLY, verifyNonce), to));
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

    static void asAnswerer(P2LNodeInternal parent, InetSocketAddress from, P2LMessage initialRequestMessage) throws Throwable {
//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.asInitiator(parent, link, outgoing);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }

        int conversationId = initialRequestMessage.conversationId;
        if (parent.connectionLimitReached()) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY), from); //do not retry refusal
            return;
        }
        if (parent.isConnectedTo(from)) {
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
            parent.graduateToEstablishedConnection(from);
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_ESTABLISHED, conversationId, P2LMessage.EXPIRE_INSTANTLY), from);
        }
    }
}