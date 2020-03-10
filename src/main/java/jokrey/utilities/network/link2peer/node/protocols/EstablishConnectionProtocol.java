package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.*;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_CONVERSATION_ID;

/**
 * IDEA:
 *   a new node does only need an internet connection and know the link of a single node with a public link
 *   using only that it can become a node to which other nodes can connect
 *
 * Types of connection establishing:
 * direct - the answerer node has a public link and can be directly accessed
 */
public class EstablishConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    //todo: mtu detection + exchange
    //todo - RE-establish connection protocol that does not do the nonce check - for historic connections (more efficient retry)
    //   todo - literally only a check if connection is in historic connections => then ping it...
    public static boolean asInitiator(P2LNodeInternal parent, P2Link to) {
        try {
            return asInitiator(parent, to, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT);
        } catch (IOException e) {
            return false;
        }
    }
    public static boolean asInitiator(P2LNodeInternal parent, P2Link to, int attempts, int initialTimeout) throws IOException {
        if(to.isPrivateLink())
            throw new IllegalArgumentException("cannot connect to private link");
        else if(to.isHiddenLink()) {
            P2Link self = parent.getSelfLink();
            boolean relayAvailable = !to.getRelayLink().equals(self) && to.getRelayLink().getSocketAddress()!=null;
            if(self.isPrivateLink() || self.isHiddenLink()) { //self should always be private or public, but it is possible to manually set it to a hidden link
                //todo - this scenario could not be realistically tested as of yet AND cannot be tested automatically
                for(int i=0;i<attempts;i++) {
                    //attempt direct connection to the link the relay server sees
                    if(asInitiatorDirect(parent, to))
                        return true;
                    //if that does not work, request a reverse connection to the link the relay server sees of this node (either one should be the correct outside nat address)
                    if(relayAvailable && asInitiatorRequestReverseConnection(parent, to.getRelaySocketAddress(), to, false, initialTimeout))
                        return false;
                    initialTimeout*=2;
                }
                return false;
            } else {
                if(!relayAvailable)
                    return asInitiatorDirect(parent, to);
                else
                    return asInitiatorRequestReverseConnection(parent, to.getRelaySocketAddress(), to, true, initialTimeout);
            }
        } else {// if(to.isPublicLink()) {
            return asInitiatorDirect(parent, to);
        }
    }

    //GUARANTEED DIFFERENT CONVERSATION IDS FOR INITIAL DIRECT AND REVERSE CONNECTIONS, BECAUSE OF:
    // theoretical problem: coincidentally it could be possible that a peer initiates a connection using the same conversationId (SOLUTION: NEGATIVE AND POSITIVE CONVERSATION IDS)
    public static short createConversationForInitialDirect(P2LNodeInternal parent) {
        return (short) -abs(parent.createUniqueConversationId()); //ALWAYS NEGATIVE
    }
    public static short createConversationForReverse(P2LNodeInternal parent) {
        short conversationId;
        do {
            conversationId = parent.createUniqueConversationId();
        } while(conversationId==Short.MIN_VALUE); //abs(min value) == min value, therefore min value illegal here
        return abs(conversationId); //ALWAYS POSITIVE
    }
    public static short abs(short a) {
        return (a < 0) ? (short) -a : a;
    }

    private static boolean asInitiatorRequestReverseConnection(P2LNodeInternal parent, SocketAddress relaySocketAddress, P2Link to, boolean giveExplicitSelfLink, int timeout) throws IOException {
        System.err.println(parent.getSelfLink()+" - EstablishConnectionProtocol.asInitiatorRequestReverseConnection");
        int conversationId = createConversationForReverse(parent);
        P2LFuture<Boolean> future = new P2LFuture<>();
        parent.addConnectionEstablishedListener((link, connectConversationId) -> {
//            if(link.equals(to)) //does not work - link to us can be different than link to relay server (which we queried the to link from)
            System.out.println(parent.getSelfLink()+" - conversationId = " + conversationId);
            System.out.println(parent.getSelfLink()+" - connectConversationId = " + connectConversationId);
            if(conversationId == connectConversationId)
                future.setCompleted(true);
        });

        P2LConversation convo = parent.internalConvo(SL_RELAY_REQUEST_DIRECT_CONNECT, conversationId, relaySocketAddress);
        LIbae libae = new LIbae();
        libae.encode(
                !giveExplicitSelfLink?new byte[0]:parent.getSelfLink().getBytesRepresentation(),
                to.getBytesRepresentation());
        convo.initClose(libae.getEncodedBytes());

        return future.get(timeout);
    }
    public static void asAnswererRelayRequestReverseConnection(P2LNodeInternal parent, P2LConversation convo, P2LMessage initialRequestMessage) throws IOException {
        convo.close();

        byte[] connectToRaw = initialRequestMessage.nextVariable();
        P2Link connectTo = connectToRaw.length==0?initialRequestMessage.header.getSender():P2Link.fromBytes(connectToRaw);
        P2Link requestFrom = P2Link.fromBytes(initialRequestMessage.nextVariable());

        P2LConversation outgoingConvo = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, initialRequestMessage.header.getConversationId(), requestFrom.getSocketAddress());
        outgoingConvo.initClose(connectTo.getBytesRepresentation());
    }
    public static void asAnswererRequestReverseConnection(P2LNodeInternal parent, P2LConversation convo, P2LMessage initialRequestMessage) throws IOException {
        convo.close();
        P2Link connectTo = P2Link.fromBytes(initialRequestMessage.asBytes());
        asInitiatorDirect(parent, connectTo, initialRequestMessage.header.getConversationId()
        );
    }

    private static boolean asInitiatorDirect(P2LNodeInternal parent, P2Link to) throws IOException {
        return asInitiatorDirect(parent, to, NO_CONVERSATION_ID);
    }
    private static boolean asInitiatorDirect(P2LNodeInternal parent, P2Link to, short conversationIdOverride) throws IOException {
        if(parent.connectionLimitReached()) return false;

        short conversationId = conversationIdOverride==NO_CONVERSATION_ID?createConversationForInitialDirect(parent):conversationIdOverride;
        P2LConversation convo = parent.internalConvo(SL_DIRECT_CONNECTION_REQUEST, conversationId, to.getSocketAddress());

        byte[] verifyNonce = convo.initExpectData(toMessage(parent, convo));


        if(verifyNonce.length == 0) {
            convo.close();
            return false;
        } else if (verifyNonce.length == 4) {
            parent.graduateToEstablishedConnection(new P2LConnection(to, BitHelper.getInt32From(verifyNonce), convo.getAvRTT()), conversationId);
            convo.close();
            return true;
        } else {
            P2LMessage peerLinkMessage = convo.answerExpect(verifyNonce);
            convo.close();

            if(peerLinkMessage.getPayloadLength() > 0) {
                parent.graduateToEstablishedConnection(fromMessage(parent, peerLinkMessage, convo.getAvRTT()), conversationId);
                return true;
            } else
                return false;
        }
    }

    public static void asAnswererDirect(P2LNodeInternal parent, P2LConversation convo, P2LMessage initialRequestMessage) throws IOException {
        P2LConnection peer = fromMessage(parent, initialRequestMessage, -1);

        if (parent.connectionLimitReached()) {
            convo.answerClose(new byte[0]);
        } else if (parent.isConnectedTo(peer.link)) {
            convo.answerClose(BitHelper.getBytes(P2LMessage.CUSTOM_RAW_SIZE_LIMIT));
        } else {
            // send a nonce to other peer
            //   if they are receiving correctly on their port they should read the nonce and be able to send it back
            //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
            byte[] nonce = new byte[16]; //must be != 4
            secureRandom.nextBytes(nonce);

            byte[] verifyNonce = convo.answerExpectData(nonce);

            if (Arrays.equals(nonce, verifyNonce)) {
                parent.graduateToEstablishedConnection(peer, initialRequestMessage.header.getConversationId());
                convo.answerClose(toMessage(parent, convo));
            } else
                convo.answerClose(new byte[0]);
        }
    }

    private static MessageEncoder toMessage(P2LNodeInternal parent, P2LConversation convo) {
        P2Link selfLink = parent.getSelfLink();
        byte[] selfLinkBytes;
        if(selfLink.isPublicLink())
            selfLinkBytes = selfLink.getBytesRepresentation();
        else
            selfLinkBytes = P2LConversation.EMPTY_BYTES;

        return MessageEncoder.encodeAll(convo.getHeaderSize(), P2LMessage.CUSTOM_RAW_SIZE_LIMIT, selfLinkBytes);
    }

    private static P2LConnection fromMessage(P2LNodeInternal parent, P2LMessage m, int avRTT) {
        int remoteBufferSize = m.nextInt();
        byte[] selfProclaimedLinkOfInitiatorRaw = m.nextVariable();
        if(selfProclaimedLinkOfInitiatorRaw==null || selfProclaimedLinkOfInitiatorRaw.length == 0)
            return new P2LConnection(P2Link.createHiddenLink(parent.getSelfLink(), m.header.getSender().getSocketAddress()), remoteBufferSize, avRTT);
        else
            return new P2LConnection(P2Link.fromBytes(selfProclaimedLinkOfInitiatorRaw), remoteBufferSize, avRTT);
    }
}