package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;

public interface P2LNodeInternal extends P2LNode {
    void graduateToEstablishedConnection(SocketAddress address);
    void markBrokenConnection(SocketAddress address, boolean retry);
    int remainingNumberOfAllowedPeerConnections();

    void sendInternalMessage(P2LMessage message, SocketAddress to) throws IOException;
    P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage message, SocketAddress to) throws IOException;
    void sendInternalMessageBlocking(P2LMessage message, SocketAddress to, int attempts, int initialTimeout) throws IOException;

    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId);
    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId, int conversationId);

    P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks);
    void notifyPacketReceivedFrom(SocketAddress from);

    void notifyUserBroadcastMessageReceived(P2LMessage message);
    void notifyUserMessageReceived(P2LMessage message);

    interface StreamReceiptReceivedListener {

    }
}
