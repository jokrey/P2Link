package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_DIRECT_CONNECTION_REQUEST;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_CONVERSATION_ID;
import static jokrey.utilities.network.link2peer.node.protocols.RelayedConnectionProtocol.createConversationForInitialDirect;

/**
 * @author jokrey
 */
public class DirectConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    public static boolean asInitiator(P2LNodeInternal parent, InetSocketAddress to) {
        return asInitiator(parent, to, NO_CONVERSATION_ID);
    }
    public static boolean asInitiator(P2LNodeInternal parent, InetSocketAddress to, short conversationIdOverride) {
        System.out.println(parent.getSelfLink()+" - DirectConnectionProtocol.asInitiator - to = " + to);
        try {
            if (parent.connectionLimitReached()) return false;
            System.out.println(parent.getSelfLink()+" - "+1);

            short conversationId = conversationIdOverride == NO_CONVERSATION_ID ? createConversationForInitialDirect(parent) : conversationIdOverride;
            P2LConversation convo = parent.internalConvo(SL_DIRECT_CONNECTION_REQUEST, conversationId, to);

            System.out.println(parent.getSelfLink()+" - "+2);
            byte[] verifyNonce = convo.initExpectData(linkToMessage(parent.getSelfLink(), convo));
            System.out.println(parent.getSelfLink()+" - verifyNonce = " + Arrays.toString(verifyNonce));

            if (verifyNonce.length == 0) {
                convo.close();
                return false;
            } else if (verifyNonce.length == 4) {
                parent.graduateToEstablishedConnection(new P2LConnection(new P2Link.Direct(to), to, BitHelper.getInt32From(verifyNonce), convo.getAvRTT()), conversationId);
                convo.close();
                System.out.println(parent.getSelfLink()+" - asInitiatorDirect success because verify nonce == 4 - [ALREADY CONNECTED]");
                return true;
            } else {
                ReceivedP2LMessage peerLinkMessage = convo.answerExpect(verifyNonce);
                convo.close();

                if (peerLinkMessage.getPayloadLength() > 0) {
                    parent.graduateToEstablishedConnection(connectionFromMessage(peerLinkMessage, peerLinkMessage.sender, convo.getAvRTT()), conversationId);
                    System.out.println(parent.getSelfLink()+" - asInitiatorDirect success because received remote self link");
                    return true;
                } else
                    return false;
            }
        } catch (TimeoutException | IOException e) {
            return false;
        }
    }


    public static void asAnswerer(P2LNodeInternal parent, P2LConversation convo, ReceivedP2LMessage initialRequestMessage) throws IOException {
        System.out.println(parent.getSelfLink()+" - DirectConnectionProtocol.asAnswerer");
        P2LConnection newPeerConnection = connectionFromMessage(initialRequestMessage, initialRequestMessage.sender, -1);
        System.out.println(parent.getSelfLink()+" - newPeerConnection = " + newPeerConnection);

        if (parent.connectionLimitReached()) {
            convo.answerClose(new byte[0]);
        } else if (parent.isConnectedTo(newPeerConnection.link)) {
            convo.answerClose(BitHelper.getBytes(P2LMessage.CUSTOM_RAW_SIZE_LIMIT));
        } else {
            // send a nonce to other peer
            //   if they are receiving correctly on their port they should read the nonce and be able to send it back
            //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
            byte[] nonce = new byte[16]; //must be != 4
            secureRandom.nextBytes(nonce);

            byte[] verifyNonce = convo.answerExpectData(nonce);

            if (Arrays.equals(nonce, verifyNonce)) {
                parent.graduateToEstablishedConnection(newPeerConnection, initialRequestMessage.header.getConversationId());
                convo.answerClose(linkToMessage(parent.getSelfLink(), convo));
            } else
                convo.answerClose(new byte[0]);
        }
    }

    private static MessageEncoder linkToMessage(P2Link selfLink, P2LConversation convo) {
        return convo.encode(selfLink.toBytes(), P2LMessage.CUSTOM_RAW_SIZE_LIMIT);
    }

    private static P2LConnection connectionFromMessage(P2LMessage mx, InetSocketAddress address, int avRTT) {
        byte[] selfProclaimedLinkOfInitiatorRaw = mx.nextVariable();
        int remoteBufferSize = mx.nextInt();
        P2Link link = P2Link.from(selfProclaimedLinkOfInitiatorRaw);
//        if(link.isOnlyLocal()) //todo - this has to happen SOMEWHERE....
//            link = ((P2Link.Local) link).toRelayed(parent.getSelfLink());
        return new P2LConnection(link, address, remoteBufferSize, avRTT);
    }
}
