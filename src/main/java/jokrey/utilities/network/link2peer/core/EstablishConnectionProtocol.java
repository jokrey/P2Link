package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
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
class EstablishConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    //todo: mtu detection + exchange
    //todo - RE-establish connection protocol that does not do the nonce check - for historic connections (more efficient retry)
    static boolean asInitiator(P2LNodeInternal parent, P2Link to) throws IOException {
        return asInitiator(parent, to, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }
    static boolean asInitiator(P2LNodeInternal parent, P2Link to, int attempts, int initialTimeout) throws IOException {
        if(to.isPrivateLink())
            throw new IllegalArgumentException("cannot connect to private link");
        else if(to.isHiddenLink()) {
            P2Link self = parent.getSelfLink();
            if(self.isPrivateLink() || self.isHiddenLink()) { //self should always be private or public, but it is possible to manually set it to a hidden link
                //todo - this scenario could not be realisticly tested as of yet AND cannot be tested automatically
                //attempt direct connection to the link the relay server sees
                if(asInitiatorDirect(parent, to, P2LNode.NO_CONVERSATION_ID, attempts, initialTimeout))
                    return true;
                //if that does not work, request a reverse connection to the link the relay server sees of this node (either one should be the correct outside nat address)
                return asInitiatorRequestReverseConnection(parent, to.getRelaySocketAddress(), to, false, attempts, initialTimeout);
            } else {
                return asInitiatorRequestReverseConnection(parent, to.getRelaySocketAddress(), to, true, attempts, initialTimeout);
            }
        } else {// if(to.isPublicLink()) {
            return asInitiatorDirect(parent, to, P2LNode.NO_CONVERSATION_ID, attempts, initialTimeout);
        }
    }

    private static boolean asInitiatorRequestReverseConnection(P2LNodeInternal parent, SocketAddress relaySocketAddress, P2Link to, boolean giveExplicitSelfLink, int attempts, int initialTimeout) throws IOException {
        int conversationId = parent.createUniqueConversationId();
        P2LFuture<Boolean> future = new P2LFuture<>();
        parent.addConnectionEstablishedListener((link, connectConversationId) -> {
//            if(link.equals(to)) //does not work - link to us can be different than link to relay server (which we queried the to link from)
//                ;
            if(conversationId == connectConversationId) //todo problem: coincidentally it could be possible that a peer initiates a connection using the same conversationId (SOLUTION: NEGATIVE AND POSITIVE CONVERSATION IDS)
                future.setCompleted(true);
        });
        parent.sendInternalMessageBlocking(P2LMessage.Factory.createSendMessageFromVariablesWithConversationId(SL_RELAY_REQUEST_DIRECT_CONNECT,
                conversationId,
                !giveExplicitSelfLink?new byte[0]:parent.getSelfLink().getBytesRepresentation(),
                to.getBytesRepresentation()), relaySocketAddress, attempts, initialTimeout);
        return future.get((long) (initialTimeout*Math.pow(2, attempts)));
    }
    static void asAnswererRelayRequestReverseConnection(P2LNodeInternal parent, P2LMessage initialRequestMessage) throws IOException {
        byte[] connectToRaw = initialRequestMessage.nextVariable();
        P2Link connectTo = connectToRaw.length==0?initialRequestMessage.header.getSender():P2Link.fromBytes(connectToRaw);
        P2Link requestFrom = P2Link.fromBytes(initialRequestMessage.nextVariable());
        parent.sendInternalMessageBlocking(P2LMessage.Factory.createSendMessage(SL_REQUEST_DIRECT_CONNECT_TO, initialRequestMessage.header.getConversationId(), connectTo.getBytesRepresentation()),
                requestFrom.getSocketAddress(), P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }
    static void asAnswererRequestReverseConnection(P2LNodeInternal parent, P2LMessage initialRequestMessage) throws IOException {
        P2Link connectTo = P2Link.fromBytes(initialRequestMessage.asBytes());
        asInitiatorDirect(parent, connectTo, initialRequestMessage.header.getConversationId(),
                P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }

    private static boolean asInitiatorDirect(P2LNodeInternal parent, P2Link to, int ovConversationId, int attempts, int initialTimeout) throws IOException {
        int conversationId = ovConversationId==P2LNode.NO_CONVERSATION_ID?parent.createUniqueConversationId():ovConversationId;
        P2Link peerLink = parent.tryReceive(attempts, initialTimeout, () -> {
            if (parent.isConnectedTo(to)) return new P2LFuture<>(to);
            return P2LFuture.before(
                    () -> parent.sendInternalMessage(selfLinkToMessage(parent, SL_DIRECT_CONNECTION_REQUEST, conversationId), to.getSocketAddress()),
                    parent.expectInternalMessage(to.getSocketAddress(), R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId))
                    .combine(message -> {
                        byte[] verifyNonce = message.asBytes();
                        if (verifyNonce.length == 0)
                            return new P2LFuture<>(to); //indicates 'already connected' todo problem is to equal to actual link
                        try {
                            return P2LFuture.before(
                                    () -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId, P2LMessage.EXPIRE_INSTANTLY, verifyNonce), to.getSocketAddress()),
                                    parent.expectInternalMessage(to.getSocketAddress(), R_DIRECT_CONNECTION_ESTABLISHED, conversationId).toType(m -> fromMessage(parent, m)));
                        } catch (Throwable e) {
                            e.printStackTrace();
                            return new P2LFuture<>(null);//causes retry!!
                        }
                    });
        });

        if (to.isHiddenLink() || to.equals(peerLink)) {
            parent.graduateToEstablishedConnection(peerLink, conversationId);
            return true;
        } else {
            System.out.println("to.equals(peerLink.get()) = " + to.equals(peerLink));
            return false;
        }
    }

    static void asAnswererDirect(P2LNodeInternal parent, SocketAddress from, P2LMessage initialRequestMessage) throws Throwable {
        P2Link peerLink = fromMessage(parent, initialRequestMessage);

        int conversationId = initialRequestMessage.header.getConversationId();
        if (parent.connectionLimitReached()) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY), from); //do not retry refusal
            return;
        }
        if (parent.isConnectedTo(peerLink)) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY, new byte[1]), from); //do not retry refusal
            return;
        }


        // send a nonce to other peer
        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
        byte[] nonce = new byte[16];
        secureRandom.nextBytes(nonce);
        //next line blocking, due to the following problem: if the packet is lost, the initiator will retry the connection:
        //     sending a new connection-request + receiving a verify nonce request WITH A DIFFERENT NONCE - however, since this came first, we will receive the nonce here...

        byte[] verifyNonce = parent.expectInternalMessage(from, R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId)
                .nowOrCancel(() ->
                        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, P2LMessage.EXPIRE_INSTANTLY, nonce), from)
                )
                .get(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT).asBytes();
        if (Arrays.equals(nonce, verifyNonce)) {
            parent.graduateToEstablishedConnection(peerLink, initialRequestMessage.header.getConversationId());
            parent.sendInternalMessage(selfLinkToMessage(parent, R_DIRECT_CONNECTION_ESTABLISHED, conversationId), from);
        }
    }

    private static P2LMessage selfLinkToMessage(P2LNodeInternal parent, int type, int conversationId) {
        P2Link selfLink = parent.getSelfLink();
        if(selfLink.isPublicLink())
            return P2LMessage.Factory.createSendMessage(type, conversationId, P2LMessage.EXPIRE_INSTANTLY, selfLink.getBytesRepresentation());
        return P2LMessage.Factory.createSendMessage(type, conversationId, P2LMessage.EXPIRE_INSTANTLY, new byte[0]);
    }
    private static P2Link fromMessage(P2LNodeInternal parent, P2LMessage m) {
        byte[] selfProclaimedLinkOfInitiatorRaw = m.asBytes();
        if(selfProclaimedLinkOfInitiatorRaw.length == 0)
            return P2Link.createHiddenLink(parent.getSelfLink(), m.header.getSender().getSocketAddress());
        else
            return P2Link.fromBytes(selfProclaimedLinkOfInitiatorRaw);
    }
}