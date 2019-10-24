package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.OutgoingHandler.Task;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.validateMsgIdNotInternal;

/**
 *
 * NOT THREAD SAFE
 *
 * @author jokrey
 */
final class P2LNodeImpl implements P2LNode, P2LNodeInternal {
    private final ConcurrentHashMap<P2Link, PeerConnection> activeConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<P2Link, Long> historicConnections = new ConcurrentHashMap<>();
    private final ArrayList<P2LMessageListener> individualMessageListeners = new ArrayList<>();
    private final ArrayList<P2LMessageListener> broadcastMessageListeners = new ArrayList<>();

    private final IncomingHandler incomingHandler;
    private final OutgoingHandler outgoingHandler;

    private final P2Link selfLink;
    private final int peerLimit;
    P2LNodeImpl(P2Link selfLink) {
        this(selfLink, Integer.MAX_VALUE);
    }
    P2LNodeImpl(P2Link selfLink, int peerLimit) {
        this.selfLink = selfLink;
        this.peerLimit = peerLimit;

        incomingHandler = new IncomingHandler(this);
        incomingHandler.startListenerThread();
        outgoingHandler = new OutgoingHandler();
    }

    @Override public P2Link getSelfLink() { return selfLink; }

    @Override public boolean isConnectedTo(P2Link peerLink) {
        return activeConnections.containsKey(peerLink);
    }

    @Override public boolean maxPeersReached() {
        return activeConnections.size() >= peerLimit;
    }

    @Override public Set<P2Link> connectToPeers(P2Link... peerLinks) {
        Set<P2Link> successLinks = new HashSet<>(peerLinks.length);
        for(P2Link peerLink : peerLinks) {
            try {
                if(! isConnectedTo(peerLink)) {
                    PeerConnection established = EstablishSingleConnectionProtocol.asRequester(this, peerLink);
                    addActiveOutgoingPeer(established);
                }
                successLinks.add(peerLink);
            } catch (IOException | EstablishSingleConnectionProtocol.RequestRefusedException e) {
                e.printStackTrace();
            }
        }
        return successLinks;
    }

    @Override public void addActiveOutgoingPeer(PeerConnection established) throws IOException {
        incomingHandler.listenToOutgoing(established);
        addActivePeer(established);
    }

    @Override public Set<P2Link> getActivePeerLinks() {
        return activeConnections.keySet();
    }

    @Override public List<P2Link> recursiveGarnerConnections(int newConnectionLimit, P2Link... setupLinks) {
        return GarnerConnectionsRecursivelyProtocol.recursiveGarnerConnections(this, newConnectionLimit, Integer.MAX_VALUE, Arrays.asList(setupLinks));
    }

    @Override public void disconnect() {
        for(PeerConnection connection:activeConnections.values()) {
            connection.tryClose();
            markBrokenConnection(connection.peerLink);
        }
    }

    @Override public P2LFuture<Integer> sendBroadcast(int msgId, byte[] message) {
        PeerConnection[] originallyActiveConnections = activeConnections.values().toArray(new PeerConnection[0]);
        if(originallyActiveConnections.length == 0)
            return new P2LFuture<>(0);

        Task[] tasks = new Task[originallyActiveConnections.length];
        for(int i=0;i<tasks.length;i++) {
            PeerConnection connection = originallyActiveConnections[i];
            tasks[i] = () -> BroadcastMessageProtocol.send(connection, this, msgId, message);
        }

        return outgoingHandler.executeAll(tasks);
    }
    @Override public P2LFuture<Boolean> sendIndividualMessageTo(P2Link peer, int msgId, byte[] message) {
        validateMsgIdNotInternal(msgId);
        PeerConnection connection = getActiveConnection(peer);
        if(connection == null)
            return new P2LFuture<>(false);

        return outgoingHandler.execute(() -> connection.trySend(msgId, message));
    }
    
    @Override public P2LFuture<P2LMessage> expectIndividualMessage(int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userIdvMessageQueue.futureFor(msgId);
    }
    @Override public P2LFuture<P2LMessage> expectIndividualMessage(P2Link fromPeer, int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userIdvMessageQueue.futureFor(fromPeer, msgId);
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userBrdMessageQueue.futureFor(msgId);
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(P2Link fromPeer, int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userBrdMessageQueue.futureFor(fromPeer, msgId);
    }

    @Override public void addIndividualMessageListener(P2LMessageListener listener) { individualMessageListeners.add(listener); }
    @Override public void addBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.add(listener); }
    @Override public void removeIndividualMessageListener(P2LMessageListener listener) { individualMessageListeners.remove(listener); }
    @Override public void removeBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.remove(listener); }



    //INTERNAL::
    @Override public int remainingNumberOfAllowedPeerConnections() {
        return peerLimit - activeConnections.size();
    }
    @Override public void addActivePeer(PeerConnection connection) throws IOException {
        PeerConnection newValue = activeConnections.computeIfAbsent(connection.peerLink, p2Link -> connection);
        if(newValue != connection) { //peerLink of connection was already known and active
            connection.tryClose();
            throw new IOException("Connection link("+connection.peerLink+") already known - this should kinda not happen if peers behave logically - selfLink: "+getSelfLink());
        }
        if(activeConnections.size() + 1 > peerLimit)
            throw new IOException("Peer limit reached");
        historicConnections.remove(connection.peerLink); //if connection was previously marked as broken, it is no longer
    }
    @Override public boolean markBrokenConnection(P2Link link) {
        PeerConnection removed = activeConnections.remove(link);
        if(removed == null) {
            System.err.println(getSelfLink() + " - link("+link+") was not found - could not mark as broken (already marked??)");
            return false;
        } else if(removed.peerLink == link)
            historicConnections.put(link, System.currentTimeMillis());
        else
            throw new IllegalStateException("removed peer connection was associated to wrong link - should never occur");
        return true;
    }
    @Override public PeerConnection getActiveConnection(P2Link peerLink) {
        return activeConnections.get(peerLink);
    }
    @Override public PeerConnection[] getActiveConnectionsExcept(P2Link... excepts) {
        ArrayList<PeerConnection> activeConnectionsExcept = new ArrayList<>();

        outer: for (PeerConnection connection : activeConnections.values()) {
            for(P2Link except: excepts)
                if (connection.peerLink.equals(except))
                    continue outer;
            activeConnectionsExcept.add(connection);
        }

        return activeConnectionsExcept.toArray(new PeerConnection[0]);
    }
    @Override public P2LFuture<P2LMessage> futureForInternal(P2Link from, int msgId) {
        return incomingHandler.internalMessageQueue.futureFor(from, msgId);
    }
    @Override public P2LFuture<Integer> executeAllOnSendThreadPool(Task[] tasks) {
        return outgoingHandler.executeAll(tasks);
    }
    @Override public void addLinkVerificationRequestPermission(P2Link link) {
        incomingHandler.addLinkVerificationRequestPermission(link);
    }
    @Override public boolean revokeVerificationRequestPermission(P2Link link) {
        return incomingHandler.revokeVerificationRequestPermission(link);
    }

    @Override public void notifyBroadcastMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyIndividualMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : individualMessageListeners) { l.received(message); }
    }
}
