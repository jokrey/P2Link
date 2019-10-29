package jokrey.utilities.network.link2peer;

import jokrey.utilities.network.link2peer.core.NodeCreator;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

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
 * !!!!!
 * TODO: light clients (clients without a public link, i.e. url + free port)
 * TODO: allow light clients to connect with each other (tcp hole punching or something like that)
 *    - does this work out of the box??
 *
 * TODO: detect stale connections by pinging every 2 minutes
 * TODO: retry broken peers in increasing intervals (5 minutes the first time, then 10, then 20, etc. NO MAX!!)
 *
 *
 *
 *
 * LATER:
 * TODO: fixed size networks (with much improved broadcast efficiency and maybe send to type(instead of send to link))
 * TODO:     improving upon fixed size: limit size networks..
 *
 * NOT_TODO: allow sending messages to a not directly connected peer (i.e. send through the network via a random search - for example useful when it is a light peer or max connections are reached)
 *       could mean a ton of traffic... (same amount of traffic as a broadcast...)
 *       can be implemented with pretty much the same efficiency using broadcasts.....
 *
 *
 *
 * FUNCTIONALITY:
 *    allows establishing connections to socket addresses ( temp becomes established connection )
 *        ping all established connections every 2 minutes
 *        allow disconnecting from established connections
 *        allow broadcasting to all established connections
 *    allows asking socket addresses for own ip (temp/potential connection)
 *    allows asking socket addresses for their established connections (temp/potential connection)
 *    allows sending individual messages to socket addresses (temp/potential connection - with optional received receipt)
 *        for receipt messages it allows a retry functionality after which an exception is thrown and (if the connection was an established one) the connection is marked as broken
 *      both internal and user messages can use this functionality.
 *    allows maintaining broken/previously-established connections
 *
 * @author jokrey
 */
public interface P2LNode {
    /**
     * @param port if self link is only a port, i.e. the public ip is not currently known
     *                 then the node will automatically fill that information when connection to the first peer
     *                 (note that this is final, if the first peer lies then no external node can ever connect to this peer,
     *                 but it will be detected when connecting to other peers since they will reject the peer connection request since the self given link resolves to a different ip)
     * @return a new node at the given self link
     * @throws IOException
     */
    static P2LNode create(int port) throws IOException { return NodeCreator.create(port); }
    static P2LNode create(int port, int peerLimit) throws IOException { return NodeCreator.create(port, peerLimit); }


    int getPort();
    /**
     * @return currently active peer links, the links can be used as ids to identify individual peer nodes
     * Note: It is not guaranteed that any peer in the returned set is still active when used.
     */
    Set<SocketAddress> getEstablishedConnections();

    boolean connectionLimitReached();

    P2LFuture<Boolean> establishConnection(SocketAddress to);
//    {
//        return establishConnections(to).toBooleanFuture(success -> success.size()==1 && success.contains(to));
//    }
    /**
     * Internally connects to given peer links
     * Returns list of successful connections (will not throw an exception for unsuccessful attempts)
     * If the connection is already active, it is returned in the success link - but the connection is not reestablished and not tested
     * returned list should be in returned set of {@link #getEstablishedConnections()}, however it is possible that the connection drops in the meantime
     * @param addresses to connect to
     * @return list of new(!), successful connections
     */
    P2LFuture<Set<SocketAddress>> establishConnections(SocketAddress... addresses);

    /**
     * Answers the question of whether this node currently maintains an active connection to the given link.
     * @param peerLink
     * @return
     */
    boolean isConnectedTo(SocketAddress peerLink);

    void disconnectFrom(SocketAddress address);
//    Future<Boolean> disconnectFromWithReceipt(SocketAddress address);

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
    List<SocketAddress> recursiveGarnerConnections(int newConnectionLimit, SocketAddress... setupLinks);


    /**
     * @param message message to be send, sender field can be null in that case it will be filled automatically
     * @return a future that is set to complete when attempts were made to send to all
     * @throws IllegalArgumentException if the message has an invalid sender(!= null and not equal to getSelfLink())
     */
    P2LFuture<Boolean> sendBroadcastWithReceipts(P2LMessage message);
    P2LFuture<P2LMessage> expectBroadcastMessage(int msgId);
    P2LFuture<P2LMessage> expectBroadcastMessage(String from, int msgId);

    void sendMessage(SocketAddress to, P2LMessage message) throws IOException;
    P2LFuture<Boolean> sendMessageWithReceipt(SocketAddress to, P2LMessage message) throws IOException;
    void sendMessageBlocking(SocketAddress to, P2LMessage message, int retries, int initialTimeout) throws IOException; //initial timeout is doubled
    P2LFuture<P2LMessage> expectMessage(int msgId);
    P2LFuture<P2LMessage> expectMessage(SocketAddress address, int msgId);
    P2LFuture<P2LMessage> expectMessage(SocketAddress address, int msgId, int conversationId);

    void addMessageListener(P2LMessageListener listener);
    void addBroadcastListener(P2LMessageListener listener);
    void addNewConnectionListener(Consumer<SocketAddress> listener);
    void removeMessageListener(P2LMessageListener listener);
    void removeBroadcastListener(P2LMessageListener listener);
    void removeNewConnectionListener(Consumer<SocketAddress> listener);

    interface P2LMessageListener {
        void received(P2LMessage message);
    }


    /**
     * Gracefully closes all connections to peers and makes sure to add them to the list of historic connections.
     * Should be used when closing the application or
     */
    default void disconnectFromAll() {
        for(SocketAddress connectionLink: getEstablishedConnections()) {
            disconnectFrom(connectionLink);
        }
    }



    default <T> T tryReceive(int retries, int initialTimeout, Request<T> f) throws IOException {
        int timeout = initialTimeout;
        for(int retryCounter=0; retryCounter<retries; retryCounter++) {
            try {
                T gotten = f.request().getOrNull(timeout);
                if (gotten != null) return gotten;
            } catch (IOException ignore) {}
            timeout *= 2;
        }
        throw new IOException(getPort()+" could not get result after "+retries+" retries");
    }
    default void tryComplete(int retries, int initialTimeout, Request<Boolean> f) throws IOException {
        int timeout = initialTimeout;
        for(int retryCounter=-1; retryCounter<retries; retryCounter++) {
            try {
                Boolean success = f.request().getOrNull(timeout);
//                System.out.println(getPort()+" "+Thread.currentThread().getId()+" - tryComplete: success = "+success+", retryCounter="+retryCounter);
                if(success!=null && success) return;
            } catch (IOException ignore) {ignore.printStackTrace();}
            timeout *= 2;
        }
        throw new IOException(getPort()+" could not get result after "+retries+" retries");
    }
    interface Request<T> {
        P2LFuture<T> request() throws IOException;
    }



    int NO_CONVERSATION_ID = 0;
}
