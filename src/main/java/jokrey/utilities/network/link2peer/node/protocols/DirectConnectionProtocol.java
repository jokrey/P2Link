package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_DIRECT_CONNECTION_REQUEST;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_CONVERSATION_ID;
import static jokrey.utilities.network.link2peer.node.protocols.RelayedConnectionProtocol.createConversationForInitialDirect;

/**
 * @author jokrey
 */
public class DirectConnectionProtocol {
    private static final byte REFUSED = -1;
    private static final byte REFUSED_NAME_COLLISION = -2;
    private static final byte ALREADY_CONNECTED = 1;
    private static final byte NEW_CONNECTION_ESTABLISHED = 2;

    public static P2LFuture<Boolean> asInitiator(P2LNodeInternal parent, InetSocketAddress to) {
        return asInitiator(parent, to, NO_CONVERSATION_ID);
    }
    public static P2LFuture<Boolean> asInitiator(P2LNodeInternal parent, InetSocketAddress to, short conversationIdOverride) {
        if (parent.connectionLimitReached()) return new P2LFuture<>(false);

        short conversationId = conversationIdOverride == NO_CONVERSATION_ID ? createConversationForInitialDirect(parent) : conversationIdOverride;
        P2LConversation convo = parent.internalConvo(SL_DIRECT_CONNECTION_REQUEST, conversationId, to);

        return convo.initExpectAsync(linkToMessage(parent.getSelfLink(), convo)).andThen(peerLinkMessage -> {
            boolean closeMessageSent = convo.tryClose();
            if(!closeMessageSent) return new P2LFuture<>(false);

            byte result = peerLinkMessage.nextByte();

            if (result == REFUSED || result == REFUSED_NAME_COLLISION) {
                return new P2LFuture<>(false);
            } else if (result == ALREADY_CONNECTED) {
                parent.graduateToEstablishedConnection(new P2LConnection(new P2Link.Direct(to), to, peerLinkMessage.nextInt(), convo.getAvRTT()), conversationId);
                return new P2LFuture<>(true);
            } else {
                parent.graduateToEstablishedConnection(connectionFromMessage(peerLinkMessage, peerLinkMessage.sender, convo.getAvRTT()), conversationId);
                return new P2LFuture<>(true);
            }
        });
    }


    public static void asAnswerer(P2LNodeInternal parent, P2LConversation convo, ReceivedP2LMessage initialRequestMessage) throws IOException {
        initialRequestMessage.nextByte();//skips the single byte up front
        P2LConnection newPeerConnection = connectionFromMessage(initialRequestMessage, initialRequestMessage.sender, -1);

        if (parent.connectionLimitReached()) {
            convo.answerClose(convo.encode(REFUSED));
        } else if (parent.isConnectedTo(newPeerConnection.address)) {
            convo.answerClose(convo.encode(ALREADY_CONNECTED, P2LMessage.CUSTOM_RAW_SIZE_LIMIT));
        } else if (parent.isConnectedTo(newPeerConnection.link) && !parent.isConnectedTo(newPeerConnection.address)) {
            convo.answerClose(convo.encode(REFUSED_NAME_COLLISION));
        } else {
            parent.graduateToEstablishedConnection(newPeerConnection, initialRequestMessage.header.getConversationId());
            convo.answerClose(linkToMessage(parent.getSelfLink(), convo));
        }
    }

    private static MessageEncoder linkToMessage(P2Link selfLink, P2LConversation convo) {
        return convo.encode(NEW_CONNECTION_ESTABLISHED, selfLink.toBytes(), P2LMessage.CUSTOM_RAW_SIZE_LIMIT);
    }

    private static P2LConnection connectionFromMessage(P2LMessage mx, InetSocketAddress address, int avRTT) {
        byte[] selfProclaimedLinkOfInitiatorRaw = mx.nextVariable();
        int remoteBufferSize = mx.nextInt();
        P2Link link = P2Link.from(selfProclaimedLinkOfInitiatorRaw);
        return new P2LConnection(link, address, remoteBufferSize, avRTT);
    }
}
