package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;

interface P2LNodeInternal extends P2LNode {
    void graduateToEstablishedConnection(SocketAddress address);
    void markBrokenConnection(SocketAddress address, boolean retry);
    int remainingNumberOfAllowedPeerConnections();

    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId);
    P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId, int conversationId);
    P2LFuture<Boolean> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks);

    /**
     * Sends a udp packet to the given peer link.
     * The peer link has to be an active connection and will be resolved to said active connection before sending.
     * Note that when this method returns the message will not necessarily be received by the receiver. In udp a packet can be lost and the sender will be none the wiser.
     *    Should it be required that the message is received, the sender should wait for a receipt from the receiver.
     *    The network does not currently support that. Instead the protocol has to be implemented by the user(VERY SIMPLE) TODO: build in this functionality.
     * @param message to be send
     * @param to receiver link
     * @throws IOException if sending fails
     */
    void sendInternalMessage(P2LMessage message, SocketAddress receiver) throws IOException;
    P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage message, SocketAddress receiver) throws IOException;
    void sendInternalMessageBlocking(P2LMessage message, SocketAddress receiver, int retries, int initialTimeout) throws IOException;

    void notifyBroadcastMessageReceived(P2LMessage message);
    void notifyMessageReceived(P2LMessage message);

    int createUniqueConversationId();
}
