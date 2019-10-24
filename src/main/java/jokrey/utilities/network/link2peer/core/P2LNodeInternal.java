package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;

interface P2LNodeInternal extends P2LNode {
    void addActivePeer(PeerConnection connection) throws IOException;
    void addActiveOutgoingPeer(PeerConnection established) throws IOException;
    default void addActiveIncomingPeer(PeerConnection established) throws IOException {
        addActivePeer(established);
    }

    boolean markBrokenConnection(P2Link link);
    int remainingNumberOfAllowedPeerConnections();
    PeerConnection getActiveConnection(P2Link peerLink);
    PeerConnection[] getActiveConnectionsExcept(P2Link... excepts);

    P2LFuture<P2LMessage> futureForInternal(P2Link from, int msgId);
    P2LFuture<Integer> executeAllOnSendThreadPool(OutgoingHandler.Task[] tasks);

    void addLinkVerificationRequestPermission(P2Link link);
    boolean revokeVerificationRequestPermission(P2Link link);

    void notifyBroadcastMessageReceived(P2LMessage message);
    void notifyIndividualMessageReceived(P2LMessage message);
}
