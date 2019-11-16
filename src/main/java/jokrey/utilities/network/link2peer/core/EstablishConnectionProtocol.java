package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.function.Supplier;

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
    static boolean asInitiator(P2LNodeInternal parent, P2Link to) {
        return asInitiator(parent, to, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }
    static boolean asInitiator(P2LNodeInternal parent, P2Link to, int attempts, int initialTimeout) {
        if(to.isPrivateLink())
            throw new IllegalArgumentException("cannot connect to private link");
        else if(to.isHiddenLink()) {
            P2Link self = parent.getSelfLink();
            boolean relayAvailable = !to.getRelayLink().equals(self) && to.getRelayLink().getRelaySocketAddress()!=null;
            if(self.isPrivateLink() || self.isHiddenLink()) { //self should always be private or public, but it is possible to manually set it to a hidden link
                //todo - this scenario could not be realisticly tested as of yet AND cannot be tested automatically
                for(int i=0;i<attempts;i++) {
                    //attempt direct connection to the link the relay server sees
                    if(asInitiatorDirect(parent, to, ()->createConversationForInitialDirect(parent), 1, initialTimeout))
                        return true;
                    //if that does not work, request a reverse connection to the link the relay server sees of this node (either one should be the correct outside nat address)
                    if(relayAvailable && asInitiatorRequestReverseConnection(parent, to.getRelaySocketAddress(), to, false, 1, initialTimeout))
                        return false;
                    initialTimeout*=2;
                }
                return false;
            } else {
                if(!relayAvailable)
                    return asInitiatorDirect(parent, to, ()->createConversationForInitialDirect(parent), 1, initialTimeout);
                else
                    return asInitiatorRequestReverseConnection(parent, to.getRelaySocketAddress(), to, true, attempts, initialTimeout);
            }
        } else {// if(to.isPublicLink()) {
            return asInitiatorDirect(parent, to, ()->createConversationForInitialDirect(parent), attempts, initialTimeout);
        }
    }

    //GUARANTEED DIFFERENT CONVERSATION IDS FOR INITIAL DIRECT AND REVERSE CONNECTIONS, BECAUSE OF:
    // theoretical problem: coincidentally it could be possible that a peer initiates a connection using the same conversationId (SOLUTION: NEGATIVE AND POSITIVE CONVERSATION IDS)
    private static int createConversationForInitialDirect(P2LNodeInternal parent) {
        return - Math.abs(parent.createUniqueConversationId()); //ALWAYS NEGATIVE
    }
    private static int createConversationForReverse(P2LNodeInternal parent) {
        int conversationId;
        do {
            conversationId = parent.createUniqueConversationId();
        } while(conversationId==Integer.MIN_VALUE); //abs(min value) == min value, therefore min value illegal here
        return Math.abs(conversationId); //ALWAYS POSITIVE
    }

    private static boolean asInitiatorRequestReverseConnection(P2LNodeInternal parent, SocketAddress relaySocketAddress, P2Link to, boolean giveExplicitSelfLink, int attempts, int initialTimeout) {
        int conversationId = createConversationForReverse(parent);
        P2LFuture<Boolean> future = new P2LFuture<>();
        parent.addConnectionEstablishedListener((link, connectConversationId) -> {
//            if(link.equals(to)) //does not work - link to us can be different than link to relay server (which we queried the to link from)
            if(conversationId == connectConversationId)
                future.setCompleted(true);
        });
        boolean success = parent.sendInternalMessageWithRetries(P2LMessage.Factory.createSendMessageFromVariablesWithConversationId(SL_RELAY_REQUEST_DIRECT_CONNECT,
                conversationId,
                !giveExplicitSelfLink?new byte[0]:parent.getSelfLink().getBytesRepresentation(),
                to.getBytesRepresentation()), relaySocketAddress, attempts, initialTimeout);
        return success && future.get((long) (initialTimeout*Math.pow(2, attempts)));
    }
    static boolean asAnswererRelayRequestReverseConnection(P2LNodeInternal parent, P2LMessage initialRequestMessage) {
        byte[] connectToRaw = initialRequestMessage.nextVariable();
        P2Link connectTo = connectToRaw.length==0?initialRequestMessage.header.getSender():P2Link.fromBytes(connectToRaw);
        P2Link requestFrom = P2Link.fromBytes(initialRequestMessage.nextVariable());
        return parent.sendInternalMessageWithRetries(P2LMessage.Factory.createSendMessage(SL_REQUEST_DIRECT_CONNECT_TO, initialRequestMessage.header.getConversationId(), connectTo.getBytesRepresentation()),
                requestFrom.getSocketAddress(), P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }
    static void asAnswererRequestReverseConnection(P2LNodeInternal parent, P2LMessage initialRequestMessage) {
        P2Link connectTo = P2Link.fromBytes(initialRequestMessage.asBytes());
        asInitiatorDirect(parent, connectTo, initialRequestMessage.header::getConversationId,
                P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
    }

    private static boolean asInitiatorDirect(P2LNodeInternal parent, P2Link to, Supplier<Integer> conversationIdSupplier, int attempts, int initialTimeout) {
        if(parent.connectionLimitReached()) return false;

        Pair<P2Link, Integer> idWhichWon = parent.tryReceiveOrNull(attempts, initialTimeout, () -> {
            Integer conversationId = conversationIdSupplier.get();
            if (parent.isConnectedTo(to)) return new P2LFuture<>(new Pair<>(to, conversationId));
            return P2LFuture.before(
                    () -> parent.sendInternalMessage(selfLinkToMessage(parent, SL_DIRECT_CONNECTION_REQUEST, conversationId), to.getSocketAddress()),
                    parent.expectInternalMessage(to.getSocketAddress(), R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId))
                    .andThen(message -> {
                        byte[] verifyNonce = message.asBytes();
                        if (verifyNonce.length == 0)
                            return new P2LFuture<>(new Pair<>(to, conversationId)); //indicates 'already connected' todo problem is 'to' equal to actual link or not? Does it matter
                        try {
                            return P2LFuture.before(
                                    () -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId, verifyNonce), to.getSocketAddress()),
                                    parent.expectInternalMessage(to.getSocketAddress(), R_DIRECT_CONNECTION_ESTABLISHED, conversationId)
                                        .toType(m -> new Pair<>(fromMessage(parent, m), conversationId)));
                        } catch (Throwable e) {
                            e.printStackTrace();
                            return new P2LFuture<>(null);//causes retry!!
                        }
                    });
        });

        //if 'to' is hidden link, then peer link will likely be a private link (i.e. unknown by peer itself) - in that case we will simply accept it
        //   in case 'to' is a public link we make sure that the link we connected to is the link the peer assumes - otherwise we are connecting over a proxy, which is fishy and we will not stand for fish
        if(idWhichWon!=null) {
            parent.graduateToEstablishedConnection(idWhichWon.l, idWhichWon.r);
            return true;
        } else {
            return false;
        }
    }

    static void asAnswererDirect(P2LNodeInternal parent, SocketAddress from, P2LMessage initialRequestMessage) throws Throwable {
        P2Link peerLink = fromMessage(parent, initialRequestMessage);

        int conversationId = initialRequestMessage.header.getConversationId();
        if (parent.connectionLimitReached()) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId), from); //do not retry refusal
            return;
        }
        if (parent.isConnectedTo(peerLink)) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, new byte[1]), from); //do not retry refusal
            return;
        }


        // send a nonce to other peer
        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
        byte[] nonce = new byte[16];
        secureRandom.nextBytes(nonce);
        //next line blocking, due to the following problem: if the packet is lost, the initiator will retry the connection:
        //     sending a new connection-request + receiving a verify nonce request WITH A DIFFERENT NONCE - however, since this came first, we will receive the nonce here...

        byte[] verifyNonce = P2LFuture.before(() ->
                parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, nonce), from),
                parent.expectInternalMessage(from, R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId))
                .get(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT).asBytes();
        if (Arrays.equals(nonce, verifyNonce)) {
            parent.graduateToEstablishedConnection(peerLink, initialRequestMessage.header.getConversationId());
            parent.sendInternalMessage(selfLinkToMessage(parent, R_DIRECT_CONNECTION_ESTABLISHED, conversationId), from);
        }
    }

    private static P2LMessage selfLinkToMessage(P2LNodeInternal parent, int type, int conversationId) {
        P2Link selfLink = parent.getSelfLink();
        if(selfLink.isPublicLink())
            return P2LMessage.Factory.createSendMessage(type, conversationId, selfLink.getBytesRepresentation());
        return P2LMessage.Factory.createSendMessage(type, conversationId, new byte[0]);
    }
    private static P2Link fromMessage(P2LNodeInternal parent, P2LMessage m) {
        byte[] selfProclaimedLinkOfInitiatorRaw = m.asBytes();
        System.out.println("selfProclaimedLinkOfInitiatorRaw = " + Arrays.toString(selfProclaimedLinkOfInitiatorRaw));
        if(selfProclaimedLinkOfInitiatorRaw.length == 0)
            return P2Link.createHiddenLink(parent.getSelfLink(), m.header.getSender().getSocketAddress());
        else
            return P2Link.fromBytes(selfProclaimedLinkOfInitiatorRaw);
    }
}