package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;

public interface P2LNodeInternal extends P2LNode {
    void graduateToEstablishedConnection(P2LConnection peer, int conversationId);
    void markBrokenConnection(P2Link address, boolean retry);
    int remainingNumberOfAllowedPeerConnections();

    void sendInternalMessage(P2LMessage message, SocketAddress to) throws IOException;
    P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage message, SocketAddress to) throws IOException;
    boolean sendInternalMessageWithRetries(P2LMessage message, SocketAddress to, int attempts, int initialTimeout);

    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId);
    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId, int conversationId);

    P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks);
    void notifyPacketReceivedFrom(SocketAddress from);

    void notifyUserBroadcastMessageReceived(P2LMessage message);
    void notifyUserMessageReceived(P2LMessage message);

    P2Link toEstablished(SocketAddress address);
}
