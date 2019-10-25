package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.OutgoingHandler.Task;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.*;
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
    private final ConcurrentHashMap<P2Link, SocketAddress> activeConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SocketAddress, P2Link> activeLinks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<P2Link, Long> historicConnections = new ConcurrentHashMap<>();
    private final ArrayList<P2LMessageListener> individualMessageListeners = new ArrayList<>();
    private final ArrayList<P2LMessageListener> broadcastMessageListeners = new ArrayList<>();

    private final IncomingHandler incomingHandler;
    private final OutgoingHandler outgoingHandler;

    private final P2Link selfLink;
    private final int peerLimit;
    P2LNodeImpl(P2Link selfLink) throws IOException {
        this(selfLink, Integer.MAX_VALUE);
    }
    P2LNodeImpl(P2Link selfLink, int peerLimit) throws IOException {
        this.selfLink = selfLink;
        this.peerLimit = peerLimit;

        incomingHandler = new IncomingHandler(this);
        outgoingHandler = new OutgoingHandler();

        new Thread(() -> {
            //todo - historic connection retry
            //todo - ping protocol
        }).start();
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
                    EstablishSingleConnectionProtocol.asRequester(this, peerLink);
                }
                successLinks.add(peerLink);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return successLinks;
    }

    @Override public Set<P2Link> getActivePeerLinks() {
//        System.out.println("activeConnections = " + activeConnections);
//        System.out.println("activeLinks = " + activeLinks);
        return activeConnections.keySet();
    }

    @Override public List<P2Link> recursiveGarnerConnections(int newConnectionLimit, P2Link... setupLinks) {
        return GarnerConnectionsRecursivelyProtocol.recursiveGarnerConnections(this, newConnectionLimit, Integer.MAX_VALUE, Arrays.asList(setupLinks));
    }

    @Override public void disconnect() {
        for(P2Link connectionLink:activeConnections.keySet()) {
            markBrokenConnection(connectionLink);
        }
    }

    @Override public P2LFuture<Integer> sendBroadcast(P2LMessage message) {
        if(message.sender != null && !message.sender.equals(getSelfLink()))
            throw new IllegalArgumentException("sender of message has to be this node's link or null");
        if(message.sender == null)
            message = message.attachSender(getSelfLink());
        validateMsgIdNotInternal(message.type);
        P2LMessage fMessage = message;

        P2Link[] originallyActivePeers = activeConnections.keySet().toArray(new P2Link[0]);
        if(originallyActivePeers.length == 0)
            return new P2LFuture<>(new Integer(0));

        Task[] tasks = new Task[originallyActivePeers.length];
        for(int i=0;i<tasks.length;i++) {
            P2Link peer = originallyActivePeers[i];
            tasks[i] = () -> {
                BroadcastMessageProtocol.send(this, peer, fMessage);
                return true;
            };
        }

        return outgoingHandler.executeAll(tasks);
    }
    @Override public P2LFuture<Boolean> sendIndividualMessageTo(P2Link peer, P2LMessage message) {
        if(message.sender != null && !message.sender.equals(getSelfLink()))
            throw new IllegalArgumentException("sender of message has to be this node's link or null");
        validateMsgIdNotInternal(message.type);

        SocketAddress connection = getActiveConnection(peer);
        if(connection == null)
            return new P2LFuture<>(false);

        try {
            send(message, connection);
            return new P2LFuture<>(true);
        } catch (IOException e) {
            return new P2LFuture<>(false);
        }
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
    @Override public void addPotentialPeer(P2Link link, SocketAddress connection) throws IOException {
//        System.out.println("P2LNodeImpl.addActivePeer");
//        System.out.println("getSelfLink() = " + getSelfLink());
//        System.out.println("link = [" + link + "], connection = [" + connection + "]");
        if(activeConnections.size() + 1 > peerLimit)
            throw new IOException("Peer limit reached");
        SocketAddress newValue = activeConnections.computeIfAbsent(link, p2Link -> connection);
        if(newValue != connection) { //peerLink of connection was already known and active
            throw new IOException("Connection link("+link+") already known - this should kinda not happen if peers behave logically - selfLink: "+getSelfLink());
        }
        activeLinks.put(connection, link);
    }
    @Override public void cancelPotentialPeer(P2Link link) throws IOException {
        SocketAddress previous = activeConnections.remove(link);
        if(previous == null)
            throw new IOException("cannot cancel unknown peer");
        activeLinks.remove(previous);
    }

    @Override public void graduateToActivePeer(P2Link link) throws IOException {
        SocketAddress connection = activeConnections.get(link);
        if(connection!=null && activeLinks.containsKey(connection))
            historicConnections.remove(link); //if connection was previously marked as broken, it is no longer
        else
            throw new IOException("cannot graduate unknown peer");
    }
    @Override public void markBrokenConnection(P2Link link) {
        SocketAddress removed = activeConnections.remove(link);
        if(removed == null) {
            System.err.println(getSelfLink() + " - link("+link+") was not found - could not mark as broken (already marked??)");
            return;
        }
        activeLinks.remove(removed);
        historicConnections.put(link, System.currentTimeMillis());
    }
    @Override public SocketAddress getActiveConnection(P2Link peerLink) {
        return activeConnections.get(peerLink);
    }
    @Override public P2Link getLinkForConnection(SocketAddress socketAddress) {
        return activeLinks.get(socketAddress);
    }
    @Override public P2LFuture<P2LMessage> futureForInternal(P2Link from, int msgId) {
        return incomingHandler.internalMessageQueue.futureFor(from, msgId);
    }
    @Override public P2LFuture<Integer> executeAllOnSendThreadPool(Task... tasks) {
        return outgoingHandler.executeAll(tasks);
    }

    @Override public void notifyBroadcastMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyIndividualMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : individualMessageListeners) { l.received(message); }
    }

    @Override public void send(P2LMessage message, P2Link to) throws IOException {
        send(message, getActiveConnection(to));
    }
    @Override public void send(P2LMessage data, SocketAddress to) throws IOException {
        DatagramPacket packet = data.getPacket();
        incomingHandler.serverSocket.send(new DatagramPacket(packet.getData(), packet.getData().length, to)); //since the server socket is bound to a port, said port will be included in the udp packet
    }
}
