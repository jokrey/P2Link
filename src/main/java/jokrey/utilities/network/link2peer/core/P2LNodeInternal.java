package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.SocketAddress;

interface P2LNodeInternal extends P2LNode {
    void addActivePeer(P2Link link, SocketAddress connection) throws IOException;

    boolean markBrokenConnection(P2Link link);
    int remainingNumberOfAllowedPeerConnections();
    SocketAddress getActiveConnection(P2Link peerLink);
    P2Link getLinkForConnection(SocketAddress socketAddress);

    P2LFuture<P2LMessage> futureForInternal(P2Link from, int msgId);
    P2LFuture<Integer> executeAllOnSendThreadPool(OutgoingHandler.Task... tasks);

    void notifyBroadcastMessageReceived(P2LMessage message);
    void notifyIndividualMessageReceived(P2LMessage message);

    P2LFuture<Boolean> sendRaw(P2LMessage message, SocketAddress receiver) throws IOException;

}
