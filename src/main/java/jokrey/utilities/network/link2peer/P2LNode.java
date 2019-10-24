package jokrey.utilities.network.link2peer;

import jokrey.utilities.network.link2peer.core.NodeCreator;
import jokrey.utilities.network.link2peer.core.P2L_Message_IDS;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.util.List;
import java.util.Set;

/**
 * This Interface is the Alpha and the Omega of the P2L Network.
 * It represents the local peer and maintains connections to remote peers.
 *
 * A Node has to provide access via a link that allows other nodes to connect to it.
 *
 * At least one peer has to be known, from that initial peer more peers can be discovered automatically.
 *
 * It knows no connections, only links. From these links messages can be received or send to.
 * Either individual messages from a specific peer.
 * Or broadcast messages that will be automatically redistributed to peers, should they not have knowledge of the message yet.
 *
 *
 *
 * TODO: detect stale connections by pinging every 2 minutes
 * TODO: fixed size networks (with much improved broadcast efficiency and maybe send to type(instead of send to link))
 * TODO:  improving upon fixed size: limit size networks..
 * TODO: retry broken peers in increasing intervals (5 minutes the first time, then 10, then 20, etc. NO MAX!!)
 *
 * !!!!!
 * TODO: light clients (clients without a public link, i.e. url + free port)
 * TODO: allow light clients to connect with each other (tcp hole punching or something like that)
 *
 * TODO: allow sending messages to a not directly connected peer (i.e. send through the network via a random search - for example useful when it is a light peer or max connections are reached)
 *       could mean a ton of traffic... (same amount of traffic as a broadcast...)
 *
 * @author jokrey
 */
public interface P2LNode {
    static P2LNode create(P2Link selfLink) { return NodeCreator.create(selfLink); }
    static P2LNode create(P2Link selfLink, int peerLimit) { return NodeCreator.create(selfLink, peerLimit); }


    /**
     * @return currently active peer links, the links can be used as ids to identify individual peer nodes
     * Note: It is not guaranteed that any peer in the returned set is still active when used.
     */
    Set<P2Link> getActivePeerLinks();

    /**
     * @return The self link of this node, or null if this node is a light client.
     */
    P2Link getSelfLink();

    boolean maxPeersReached();

    default boolean connectToPeer(P2Link peerLink) {
        Set<P2Link> success = connectToPeers(peerLink);
        return success.size()==1 && success.contains(peerLink);
    }
    /**
     * Internally connects to given peer links
     * Returns list of successful connections (will not throw an exception for unsuccessful attempts)
     * If the connection is already active, it is returned in the success link - but the connection is not reestablished and not tested
     * returned list should be in returned set of {@link #getActivePeerLinks}, however it is possible that the connection drops in the meantime
     * @param peerLinks links to connect to
     * @return list of new(!), successful connections
     */
    Set<P2Link> connectToPeers(P2Link... peerLinks);

    /**
     * Answers the question of whether this node currently maintains an active connection to the given link.
     * @param peerLink
     * @return
     */
    boolean isConnectedTo(P2Link peerLink);

    /**
     * Will establish a connection to every given setup link and request their peers.
     *
     * From then it will recursively attempt to establish connections to randomly selected received peers, until the new connection limit is reached.
     * If the connection limit is smaller than the number of setup links, not all setup links may be connected to
     *
     * The max peer limit in the constructor is being respected at all times
     *
     * @param newConnectionLimit
     * @param setupLinks
     * @return newly, successfully connected links
     */
    List<P2Link> recursiveGarnerConnections(int newConnectionLimit, P2Link... setupLinks);



    default P2LFuture<Integer> sendBroadcast(byte[] message) {
        return sendBroadcast(P2L_Message_IDS.DEFAULT_ID, message);
    }
    P2LFuture<Integer> sendBroadcast(int msgId, byte[] message);
    default P2LFuture<Boolean> sendIndividualMessageTo(P2Link peer, byte[] message) {
        return sendIndividualMessageTo(peer, P2L_Message_IDS.DEFAULT_ID, message);
    }
    P2LFuture<Boolean> sendIndividualMessageTo(P2Link peer, int msgId, byte[] message);
    P2LFuture<P2LMessage> expectIndividualMessage(int msgId);
    P2LFuture<P2LMessage> expectIndividualMessage(P2Link fromPeer, int msgId);
    P2LFuture<P2LMessage> expectBroadcastMessage(int msgId);
    P2LFuture<P2LMessage> expectBroadcastMessage(P2Link fromPeer, int msgId);

    void addIndividualMessageListener(P2LMessageListener listener);
    void addBroadcastListener(P2LMessageListener listener);
    void removeIndividualMessageListener(P2LMessageListener listener);
    void removeBroadcastListener(P2LMessageListener listener);
    interface P2LMessageListener {
        void received(P2LMessage message);
    }


    /**
     * Gracefully closes all connections to peers and makes sure to add them to the list of historic connections.
     * Should be used when closing the application or
     */
    void disconnect();
}
