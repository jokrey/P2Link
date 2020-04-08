package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LBroadcastMessage;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.conversation.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentInputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentOutputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LInputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LOutputStream;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.validateMsgTypeNotInternal;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_STEP;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

public interface P2LNodeInternal extends P2LNode {
    void graduateToEstablishedConnection(P2LConnection peer, int conversationId);
    void markBrokenConnection(InetSocketAddress address, boolean retry);
    int remainingNumberOfAllowedPeerConnections();

    void sendInternalMessage(InetSocketAddress to, P2LMessage message) throws IOException;
    P2LFuture<Boolean> sendInternalMessageWithReceipt(InetSocketAddress to, P2LMessage message) throws IOException;
    boolean sendInternalMessageWithRetries(InetSocketAddress to, P2LMessage message, int attempts) throws IOException;

    P2LFuture<ReceivedP2LMessage> expectInternalMessage(InetSocketAddress from, int type);
    P2LFuture<ReceivedP2LMessage> expectInternalMessage(InetSocketAddress from, int type, int conversationId);

    P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks);
    @Override default P2LFuture<Integer> executeThreaded(P2LThreadPool.Task... tasks) {
        return executeAllOnSendThreadPool(tasks);
    }

    void notifyPacketReceivedFrom(InetSocketAddress from);

    void registerInternalConversationFor(int type, ConversationAnswererChangeThisName handler);

    P2LConversation internalConvo(int type, int conversationId, InetSocketAddress to);

    void notifyUserBroadcastMessageReceived(P2LBroadcastMessage message);
    void notifyUserMessageReceived(ReceivedP2LMessage message);

    P2LFragmentInputStream createFragmentInputStream(InetSocketAddress from, short type, short conversationId, short step);
    default P2LFragmentInputStream createFragmentInputStream(InetSocketAddress from, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return createFragmentInputStream(from, toShort(messageType), toShort(conversationId), NO_STEP);
    }
    P2LFragmentOutputStream createFragmentOutputStream(InetSocketAddress from, short type, short conversationId, short step);
    default P2LFragmentOutputStream createFragmentOutputStream(InetSocketAddress from, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return createFragmentOutputStream(from, toShort(messageType), toShort(conversationId), NO_STEP);
    }

    void unregister(P2LInputStream stream);
    void unregister(P2LOutputStream stream);
    
    
    //DEFAULT OVERRIDES - NEWLY POSSIBLE
    default void registerConversationFor(int type, ConversationAnswererChangeThisName handler) {
        validateMsgTypeNotInternal(type);
        registerInternalConversationFor(type, handler);
    }
    default P2LConversation internalConvo(int type, InetSocketAddress to) {
        return internalConvo(type, createUniqueConversationId(), to);
    }
    default P2LConversation convo(int type, InetSocketAddress to) {
        validateMsgTypeNotInternal(type);
        return internalConvo(type, to);
    }
    default void sendMessage(InetSocketAddress to, P2LMessage message) throws IOException {
        validateMsgTypeNotInternal(message.header.getType());
        sendInternalMessage(to, message);
    }
    default P2LFuture<Boolean> sendMessageWithReceipt(InetSocketAddress to, P2LMessage message) throws IOException {
        validateMsgTypeNotInternal(message.header.getType());
        return sendInternalMessageWithReceipt(to, message);
    }
    default boolean sendInternalMessageWithRetries(P2LMessage message, InetSocketAddress to, int attempts, int initialTimeout) {
        boolean success = tryComplete(attempts, initialTimeout, () -> sendInternalMessageWithReceipt(to, message));
        if(!success)
            markBrokenConnection(to, true);
        return success;
    }
    default boolean sendMessageWithRetries(InetSocketAddress to, P2LMessage message, int attempts) throws IOException {
        validateMsgTypeNotInternal(message.header.getType());
        return sendInternalMessageWithRetries(to, message, attempts);
    }
    default boolean sendMessageWithRetries(InetSocketAddress to, P2LMessage message, int attempts, int initialTimeout) {
        validateMsgTypeNotInternal(message.header.getType());
        return sendInternalMessageWithRetries(message, to, attempts, initialTimeout);
    }
    default P2LFuture<ReceivedP2LMessage> expectMessage(InetSocketAddress from, int type) {
        validateMsgTypeNotInternal(type);
        return expectInternalMessage(from, type);
    }
    default P2LFuture<ReceivedP2LMessage> expectMessage(InetSocketAddress from, int type, int conversationId) {
        validateMsgTypeNotInternal(type);
        return expectInternalMessage(from, type, conversationId);
    }



    InterfaceAddress getLocalIPv4InterfaceAddress();
    InterfaceAddress getLocalIPv6InterfaceAddress();
}
