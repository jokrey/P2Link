package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.SocketAddress;

interface P2LNodeInternal extends P2LNode {
    void addPotentialPeer(P2Link link, SocketAddress outgoing) throws IOException;
    void cancelPotentialPeer(P2Link link) throws IOException;
    void graduateToActivePeer(P2Link link) throws IOException;
    void markBrokenConnection(P2Link link);
    int remainingNumberOfAllowedPeerConnections();
    SocketAddress getActiveConnection(P2Link peerLink);
    P2Link getLinkForConnection(SocketAddress socketAddress);

    P2LFuture<P2LMessage> futureForInternal(P2Link from, int msgId);
    P2LFuture<Integer> executeAllOnSendThreadPool(OutgoingHandler.Task... tasks);

    void notifyBroadcastMessageReceived(P2LMessage message);
    void notifyIndividualMessageReceived(P2LMessage message);

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
    void send(P2LMessage message, P2Link to) throws IOException;
    void send(P2LMessage message, SocketAddress receiver) throws IOException;
}
