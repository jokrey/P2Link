package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.stream.P2LInputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LOutputStream;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.validateMsgTypeNotInternal;

public interface P2LNodeInternal extends P2LNode {
    void graduateToEstablishedConnection(P2LConnection peer, int conversationId);
    void markBrokenConnection(P2Link address, boolean retry);
    int remainingNumberOfAllowedPeerConnections();

    void sendInternalMessage(SocketAddress to, P2LMessage message) throws IOException;
    P2LFuture<Boolean> sendInternalMessageWithReceipt(SocketAddress to, P2LMessage message) throws IOException;
    boolean sendInternalMessageWithRetries(SocketAddress to, P2LMessage message, int attempts) throws IOException;

    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int type);
    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int type, int conversationId);

    P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks);
    @Override default P2LFuture<Integer> executeThreaded(P2LThreadPool.Task... tasks) {
        return executeAllOnSendThreadPool(tasks);
    }

    void notifyPacketReceivedFrom(SocketAddress from);

    void registerInternalConversationFor(int type, ConversationAnswererChangeThisName handler);

    P2LConversationImpl internalConvo(int type, int conversationId, SocketAddress to);

    void notifyUserBroadcastMessageReceived(P2LMessage message);
    void notifyUserMessageReceived(P2LMessage message);

    P2Link toEstablished(SocketAddress address);
    P2LConnection getConnection(InetSocketAddress socketAddress);

    void unregister(P2LInputStream stream);
    void unregister(P2LOutputStream stream);
    
    
    //DEFAULT OVERRIDES - NEWLY POSSIBLE
    default void registerConversationFor(int type, ConversationAnswererChangeThisName handler) {
        validateMsgTypeNotInternal(type);
        registerInternalConversationFor(type, handler);
    }
    default P2LConversationImpl internalConvo(int type, SocketAddress to) {
        return internalConvo(type, createUniqueConversationId(), to);
    }
    default P2LConversationImpl convo(int type, SocketAddress to) {
        validateMsgTypeNotInternal(type);
        return internalConvo(type, to);
    }
    default void sendMessage(SocketAddress to, P2LMessage message) throws IOException {
        validateMsgTypeNotInternal(message.header.getType());
        sendInternalMessage(to, message);
    }
    default P2LFuture<Boolean> sendMessageWithReceipt(SocketAddress to, P2LMessage message) throws IOException {
        validateMsgTypeNotInternal(message.header.getType());
        return sendInternalMessageWithReceipt(to, message);
    }
    default boolean sendInternalMessageWithRetries(P2LMessage message, SocketAddress to, int attempts, int initialTimeout) {
        boolean success = tryComplete(attempts, initialTimeout, () -> sendInternalMessageWithReceipt(to, message));
        if(!success)
            markBrokenConnection(toEstablished(to), true);
        return success;
    }
    default boolean sendMessageWithRetries(SocketAddress to, P2LMessage message, int attempts) throws IOException {
        validateMsgTypeNotInternal(message.header.getType());
        return sendInternalMessageWithRetries(to, message, attempts);
    }
    default boolean sendMessageWithRetries(SocketAddress to, P2LMessage message, int attempts, int initialTimeout) {
        validateMsgTypeNotInternal(message.header.getType());
        return sendInternalMessageWithRetries(message, to, attempts, initialTimeout);
    }
    default P2LFuture<P2LMessage> expectMessage(SocketAddress from, int type) {
        validateMsgTypeNotInternal(type);
        return expectInternalMessage(from, type);
    }
    default P2LFuture<P2LMessage> expectMessage(SocketAddress from, int type, int conversationId) {
        validateMsgTypeNotInternal(type);
        return expectInternalMessage(from, type, conversationId);
    }
}
