package jokrey.utilities.network.link2peer;

import jokrey.utilities.network.link2peer.node.conversation.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.HistoricConnection;
import jokrey.utilities.network.link2peer.node.core.NodeCreator;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes;
import jokrey.utilities.network.link2peer.node.stream.*;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This Interface is the Alpha and the Omega of the P2L Network. The P2L Network is middleware designed to provide a useful abstraction and advanced features to the bare udp protocol.
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
 *
 * LATER:
 * TODO: fixed size networks (with much improved broadcast efficiency
 *           and maybe send to id[mpi-style](instead of send to link), though that would require potentially complex routing)
 * TODO:   improving upon fixed size: limit size networks..
 *
 *
 * NOT_TODO: support for multicast - usage instead of broadcast when multiple peers in same local network? - unlikely advantage in practical applications
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
 *    allows maintaining broken/previously-established connections
 *        broken connections will be retried at a growing interval
 *    allows asking socket addresses for own ip (temp/potential connection)
 *    allows asking socket addresses for their established connections (temp/potential connection)
 *    allows sending individual messages to socket addresses (temp/potential connection - with optional received receipt)
 *        for receipt messages it allows a retry functionality after which an exception is thrown and (if the connection was an established one) the connection is marked as broken
 *        allow breaking up messages and sending them in parts
 *        allow streaming very long messages (i.e. break up messages, but requery lost part-packets)
 *      both internal and user messages can use this functionality.
 *
 * todo - allow automatically finding mtu using icmp for established connections (require streams to be to established connections and use mtu there, mtu can be different to every node, mtu max = CUSTOM_RAW_SIZE of a peer node)
 *
 *
 * THREE TYPES OF NODES:
 *    Public - getSelfLink().isPublicLink() == true
 *        i.e. anyone can attempt to establish a connection and send messages using the dns/ip + port combination of a socket address
 *    Hidden - getSelfLink().isHidden() == true
 *        i.e. the nodes internet connection is behind a NAT and requires UDP hole punching OR reverse connection to establish a connection to other hidden or public nodes
 *    Private - getSelfLink().isPrivateLink == true
 *        Self link of hidden nodes - hidden links conceptually do not known their own ip addresses - they exclusively know their own port (and not even their public port[nat changed])
 *
 * @author jokrey
 */
public interface P2LNode {
    /**
     * @param selfLink port on which this node should listen for messages
     * @return a new node listening to the given port
     * @throws IOException if the port is unavailable
     */
    static P2LNode create(P2Link selfLink) throws IOException { return NodeCreator.create(selfLink); }

    /**
     * @param selfLink port on which this node should listen for messages
     * @param peerLimit final peer limit for this node, this node will never maintain more connections that the given limit - any more connections will be not established or rejected
     * @return a new node listening to the given port
     * @throws IOException if the port is unavailable
     */
    static P2LNode create(P2Link selfLink, int peerLimit) throws IOException { return NodeCreator.create(selfLink, peerLimit); }


    /** @return the port on which this node is listening - an ip or dns plus this port and be used to connect to this node */
    P2Link getSelfLink();
    void setSelfLink(P2Link link);

    /** Irreversibly closes this node and unbinds its server socket */
    void close();

    /** Prints all debug information */
    void printDebugInformation();

    /**
     * Creates a unique conversation id(terms and conditions apply - i.e. cycles after 2^32-1 calls).
     * Guaranteed to never equal {@link jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader#NO_CONVERSATION_ID}, i.e. 0.
     * Conversation id's are used to create unambiguous message requests in conversations.
     * When multiple conversations of the same type are run concurrently, for example through retrying after dropped packages or in slow connections,
     *    then it is required to still handle them separately.
     * Conversation id's should be created and send by the conversation initiator.
     * @return the created id
     */
    short createUniqueConversationId();


    /**
     * @return currently active peer links, the links can be used as ids to identify individual peer nodes
     * Note: It is not guaranteed that any peer in the returned set is still active when used.
     * DO NOT MODIFY RETURNED SET
     */
    P2LConnection[] getEstablishedConnections();
    /**
     * @return addresses of peers that this node had previously maintained an established connection, the connections however have since timed out or proven to be unreliable and were therefore closed
     */
    HistoricConnection[] getPreviouslyEstablishedConnections();

    /** @return whether any more connections can be established or the final limit has already been reached */
    boolean connectionLimitReached();

    /**
     * Attempts to establish a connection to the given address.
     * Returns true when the connection was already established
     * @param to address of the node to establish a connection to
     * @return a future of whether it was possible to establish the connection to parameter to
     */
    P2LFuture<Boolean> establishConnection(P2Link to);
    /**
     * Attempts to establish a connection to the given addresses.
     * Returns future of a subset of connections from given addresses to which the node now maintains a connection.
     * If the connection is already active, it is returned in the successful connection - but the connection is not reestablished and not tested
     * returned set should be as subset of {@link #getEstablishedConnections()}, however it is possible that the connection drops in the meantime
     * @param addresses to connect to
     * @return future set of established given connections
     */
    P2LFuture<List<P2Link>> establishConnections(P2Link... addresses);

    /**
     * @param to is connected to?
     * @return whether this node is connected to to
     */
    boolean isConnectedTo(P2Link to);
    /**
     * @param to is connected to?
     * @return whether this node is connected to to
     */
    boolean isConnectedTo(InetSocketAddress to);
    InetSocketAddress resolve(P2Link link);
    InetSocketAddress resolveByName(String link);
    P2LConnection getConnection(P2Link link);
    P2LConnection getConnection(InetSocketAddress address);

    /**
     * Sends a disconnect request to the node at the given address.
     * Additionally, it marks from as a broken connection and removes it from established connections.
     * @param from node address to disconnect from
     */
    default void disconnectFrom(P2LConnection from) {
        disconnectFrom(from.address);
    }
    /**@see #disconnectFrom(P2LConnection) */
    default void disconnectFrom(P2Link link) {
        disconnectFrom(resolve(link));
    }
    void disconnectFrom(InetSocketAddress address);

    /**
     * Gracefully closes all connections to peers and makes sure to add them to the list of historic connections.
     * Should be used when closing the application or
     */
    default void disconnectFromAll() {
        for(P2LConnection connectionLink: getEstablishedConnections()) {
            disconnectFrom(connectionLink);
        }
    }


    default P2LFuture<List<P2Link>> recursiveGarnerConnections(int newConnectionLimit, P2Link... setupLinks) {
        return recursiveGarnerConnections(newConnectionLimit, Integer.MAX_VALUE, setupLinks);
    }
    P2LFuture<List<P2Link>> recursiveGarnerConnections(int newConnectionLimit, int newConnectionLimitPerRecursion, P2Link... setupLinks);

    default P2LFuture<List<P2Link>> queryKnownLinksOf(P2Link from) {
        return queryKnownLinksOf(resolve(from));
    }
    P2LFuture<List<P2Link>> queryKnownLinksOf(InetSocketAddress from);


    /**
     * Queries and returns the p2link of this node as it is visible to the specified peer.
     * The returned link is always a public link, despite the fact that it may not be a public ip - or even registered with the specified peer as a public link.
     * However that distinction cannot be easily made from that point of view.
     *
     * If the returned address is determined to be a public ip, it can be set as the self link of this node.
     *
     * @param requestFrom the raw address to request the ip from
     * @return a future for the requested self link as seen by the specified peer
     * @throws IOException if the send went to garbage
     */
    P2LFuture<P2Link.Direct> whoAmI(InetSocketAddress requestFrom) throws IOException;

    /**
     * Sends the given message to the given address.
     * Non-Blocking. After this methods returns it is not guaranteed that the receiver has or will ever receive the send message(udp maybe semantic).
     * @param to address to send to
     * @param message message to send
     * @throws IOException if the send went to garbage
     */
    void sendMessage(InetSocketAddress to, P2LMessage message) throws IOException;
    /**@see #sendMessage(InetSocketAddress, P2LMessage)*/
    default void sendMessage(P2Link to, P2LMessage message) throws IOException {
        sendMessage(resolve(to), message);
    }

    /**
     * Sends the given message to the given address and request a receipt.
     * Non-Blocking. After this methods returns it is not guaranteed that the receiver has or will ever receive the send message(udp maybe semantic).
     * The returned future represents request for a received receipt of the message.
     * Not guaranteed to ever complete. If either send or receipt package is lost, the future will never complete.
     * However if the future never completes, this does not indicate that the other node has not received and handled the message.
     * It is possible that only the receipt package has been lost.
     * @param to address to send to
     * @param message message to send
     * @return a future indicating the receival of a receipt for the send message
     * @throws IOException if the send went to garbage
     */
    P2LFuture<Boolean> sendMessageWithReceipt(InetSocketAddress to, P2LMessage message) throws IOException;
    /**@see #sendMessageWithReceipt(InetSocketAddress, P2LMessage)*/
    default P2LFuture<Boolean> sendMessageWithReceipt(P2Link to, P2LMessage message) throws IOException {
        return sendMessageWithReceipt(resolve(to), message);
    }

    /**
     * Sends the given message to the given address.
     * Additionally, block as long as no receipt for the given message has been received.
     * Additionally, retry a given number of times after a given timeout. After each retry the given initial timeout is doubled (compare tcp's reasoning for a similar behaviour).
     * If the the receipt was still not received after all retries, the connection is marked as broken.
     *
     * CURRENTLY: no handling of double message. Double received message in the context of a retry are handled twice (in case the receive drops)
     *    work around has to be 'manual' using a conversation id
     *
     * @param to address to send to
     * @param message message to send
     * @param attempts total number of attempts (i.e. 0 will mean no attempt will be made at all)
     * @param initialTimeout initial timeout - since doubled with each retry, max timeout is: (initialTimeout * 2^retries)
     * @throws IOException if any send went to garbage
     */
    boolean sendMessageWithRetries(InetSocketAddress to, P2LMessage message, int attempts, int initialTimeout) throws IOException;
    /**@see #sendMessageWithRetries(InetSocketAddress, P2LMessage, int, int)*/
    default void sendMessageWithRetries(P2Link to, P2LMessage message, int attempts, int initialTimeout) throws IOException {
        sendMessageWithRetries(resolve(to), message, attempts, initialTimeout);
    }
    boolean sendMessageWithRetries(InetSocketAddress to, P2LMessage message, int attempts) throws IOException;
    /**@see #sendMessageWithRetries(InetSocketAddress, P2LMessage, int, int)*/
    default void sendMessageWithRetries(P2Link to, P2LMessage message, int attempts) throws IOException {
        sendMessageWithRetries(resolve(to), message, attempts);
    }


    /**
     * Creates a future for an expected message with the given messageType.
     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @return the created future
     */
    P2LFuture<ReceivedP2LMessage> expectMessage(int messageType);
    /**
     * Creates a future for an expected message with the given sender and messageType.
     * @param from the sender of the broadcast message (decoded from the raw ip packet)
     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @return the created future
     */
    P2LFuture<ReceivedP2LMessage> expectMessage(InetSocketAddress from, int messageType);
    /**@see #expectMessage(InetSocketAddress, int)*/
    default P2LFuture<ReceivedP2LMessage> expectMessage(P2Link from, int messageType) {
        return expectMessage(resolve(from), messageType);
    }

    /**
     * Creates a future for an expected message with the given sender, messageType and conversationId (see {@link #createUniqueConversationId()}).
     * @param from the sender of the broadcast message (decoded from the raw ip packet)
     * @param type a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @param conversationId the conversation id of the message
     * @return the created future
     */
    P2LFuture<ReceivedP2LMessage> expectMessage(InetSocketAddress from, int type, int conversationId);
    /**@see #expectMessage(InetSocketAddress, int, int)*/
    default P2LFuture<ReceivedP2LMessage> expectMessage(P2Link from, int messageType, int conversationId) {
        return expectMessage(resolve(from), messageType, conversationId);
    }

    /**
     * @param message message to be send, sender field can be null in that case it will be filled automatically
     * @return a future that is set to complete when attempts were made to send to all
     * @throws IllegalArgumentException if the message has an invalid sender(!= null and not equal to getSelfLink())
     */
    P2LFuture<Integer> sendBroadcastWithReceipts(P2LMessage message);
    /**
     * Creates a future for an expected broadcast message with the given messageType.
     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @return the created future
     */
    P2LFuture<P2LBroadcastMessage> expectBroadcastMessage(int messageType);
    /**
     * Creates a future for an expected broadcast message with the given source and messageType.
     * @param source source of the expected message if a relayed link of some name is expected the local link can also be used, since they equal when their names equal
     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @return the created future
     */
    P2LFuture<P2LBroadcastMessage> expectBroadcastMessage(P2Link source, int messageType);
//    /**
//     * Creates a future for an expected broadcast message with the given sender and messageType.
//     * @param from the self named sender of the broadcast message (never validated to be anything)
//     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
//     * @return the created future
//     */
//    P2LFuture<P2LMessage> expectBroadcastMessage(String from, int messageType);

    /**
     * Returns the stream for the given identifier. It is possible to have up to (2^15-1) * (2^15-1) streams from a single source
     * The multiple streams can be used for comfortable parallel download or communication without establishing multiple, 'real' connections.
     * @param from the sender of the broadcast message (decoded from the raw ip packet)
     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @param conversationId the conversation id of the message
     * @return a stream representation of the connection - with the associated guarantees
     * @see P2LOrderedInputStream
     */
    P2LOrderedInputStream createInputStream(InetSocketAddress from, int messageType, int conversationId);
    /**@see #createInputStream(InetSocketAddress, int, int)*/
    default P2LOrderedInputStream createInputStream(P2Link from, int messageType, int conversationId) {
        return createInputStream(resolve(from), messageType, conversationId);
    }
    boolean registerCustomInputStream(InetSocketAddress from, int messageType, int conversationId, P2LInputStream inputStream);
    boolean registerCustomOutputStream(InetSocketAddress to, int messageType, int conversationId, P2LOutputStream outputStream);

    /**
     * Returns the stream for the given identifier. It is possible to have up to (2^15-1) * (2^15-1) streams from a single source
     * The multiple streams can be used for comfortable parallel upload or communication without establishing multiple, 'real' connections.
     *
     * Note: Before a peer can receive data {@link #createInputStream(InetSocketAddress, int, int)} has to be called with the same typ-conversationId combination on the peer side.
     * Before that has occurred it is useless to send data. Appropriate synchronization remains the responsibility of the application.
     *
     * Tcp-like, most simple synchronization would be to call both {@link #createInputStream(InetSocketAddress, int, int)} and {@link #createOutputStream(InetSocketAddress, int, int)} when the connection is established (using {@link #addConnectionEstablishedListener(BiConsumer)}).
     *
     * @param to the intended receiver of the stream - does not currently have to be an established connection but might have to be in the future
     * @param messageType a message type of user privileges (i.e. that {@link P2LInternalMessageTypes#isInternalMessageId(int)} does not hold)
     * @param conversationId the conversation id of the message
     * @return a stream representation of the connection - with the associated guarantees
     * @see P2LOrderedOutputStream
     */
    P2LOrderedOutputStream createOutputStream(InetSocketAddress to, int messageType, int conversationId);
    /**@see #createOutputStream(InetSocketAddress, int, int)*/
    default P2LOrderedOutputStream createOutputStream(P2Link to, int messageType, int conversationId) {
        return createOutputStream(resolve(to), messageType, conversationId);
    }

    /**@see #createInputStream(InetSocketAddress, int, int)*/
    P2LFragmentInputStream createFragmentInputStream(InetSocketAddress from, int messageType, int conversationId);
    /**@see #createInputStream(InetSocketAddress, int, int)*/
    default P2LFragmentInputStream createFragmentInputStream(P2Link from, int messageType, int conversationId) {
        return createFragmentInputStream(resolve(from), messageType, conversationId);
    }
    /**@see #createOutputStream(InetSocketAddress, int, int)*/
    P2LFragmentOutputStream createFragmentOutputStream(InetSocketAddress to, int messageType, int conversationId);
    /**@see #createOutputStream(InetSocketAddress, int, int)*/
    default P2LFragmentOutputStream createFragmentOutputStream(P2Link to, int messageType, int conversationId) {
        return createFragmentOutputStream(resolve(to), messageType, conversationId);
    }

    /**
     * Retry feature for more complex conversations.
     * Conversations with a retry feature should always use a conversation id (see {@link #createUniqueConversationId()}).
     * @param attempts total number of attempts (i.e. 0 will mean no attempt will be made at all)
     * @param initialTimeout initial timeout - since doubled with each retry, max timeout is: (initialTimeout * 2^retries)
     * @param conversationWithResult function that produces future which represents a result.
     *                               Complex conversations will likely want to chain that future, with each waiting for a message as a combined future.
     *                               For this purpose {@link P2LFuture#andThen(Function)} can be used.
     * @return the received final result
     * @throws IOException if no result could be obtained after given number of retries or the send went to garbage
     */
    default <T> T tryReceive(int attempts, int initialTimeout, Request<T> conversationWithResult) throws IOException {
        Throwable t = null;
        int timeout = initialTimeout;
        for(int attempt=0; attempt<attempts; attempt++) {
            try {
                T gotten = conversationWithResult.request().getOrNull(timeout);
                if (gotten != null) return gotten;
            } catch (Throwable thrown) {t=thrown;}
            timeout *= 2;
        }
        throw new IOException(getSelfLink()+" could not get result after "+attempts+" attempts", t);
    }
    /**
     * Retry feature for more complex conversations.
     * Conversations with a retry feature should always use a conversation id (see {@link #createUniqueConversationId()}).
     * @param attempts total number of attempts (i.e. 0 will mean no attempt will be made at all)
     * @param initialTimeout initial timeout - since doubled with each retry, max timeout is: (initialTimeout * 2^retries)
     * @param conversationWithResult function that produces future which represents a result.
     *                               Complex conversations will likely want to chain that future, with each waiting for a message as a combined future.
     *                               For this purpose {@link P2LFuture#andThen(Function)} can be used.
     * @return the received final result or null (if no result could be obtained after given number of retries or the send went to garbage)
     */
    default <T> T tryReceiveOrNull(int attempts, int initialTimeout, Request<T> conversationWithResult) {
        Throwable t = null;
        int timeout = initialTimeout;
        for(int attempt=0; attempt<attempts; attempt++) {
            try {
                T gotten = conversationWithResult.request().getOrNull(timeout);
                if (gotten != null) return gotten;
            } catch (Throwable thrown) {t=thrown;}
            timeout *= 2;
        }
        return null;
    }
    /**
     * Like {@link #tryReceive(int, int, Request)}, except that the conversation produces a boolean representing success.
     * Unlike {@link #tryReceive(int, int, Request)} this method allows triggering a retry early, by setting the returned future to false.
     * @param attempts total number of attempts (i.e. 0 will mean no attempt will be made at all)
     * @param initialTimeout initial timeout - since doubled with each retry, max timeout is: (initialTimeout * 2^retries)
     * @param conversation function that can succeed, but if it does not can be at least retried.
     * @return whether the operation completed successfully
     */
    default boolean tryComplete(int attempts, int initialTimeout, Request<Boolean> conversation) {
        Throwable t = null;
        int timeout = initialTimeout;
        for(int attempt=0; attempt<attempts; attempt++) {
            try {
                Boolean success = conversation.request().getOrNull(timeout);
                if(success!=null && success) return true;
            } catch (Throwable thrown) {t=thrown;}
            timeout *= 2;
        }
        return false;
    }

    /** Function producing something in the future */
    interface Request<T> { P2LFuture<T> request() throws Throwable;}


    void registerConversationFor(int type, ConversationAnswererChangeThisName handler);
    default P2LConversation convo(int type, P2Link to) {
        return convo(type, resolve(to));
    }
    P2LConversation convo(int type, InetSocketAddress to);

    P2LFuture<Integer> executeThreaded(P2LThreadPool.Task... tasks);

    /**
     * This method provides another possibility of asynchronously receiving messages.
     * All user level message will be received by the given listener.
     * The message will nonetheless remain receivable by the more exact 'expect' futures.
     * @param listener listener to add
     */
    void addMessageListener(P2LMessageListener<ReceivedP2LMessage> listener);
    /**
     * This method provides another possibility of asynchronously receiving broadcasts.
     * All user level broadcasts will be received by the given listener.
     * The broadcast will nonetheless remain receivable by the more exact 'expect' futures.
     * @param listener listener to add
     */
    void addBroadcastListener(P2LMessageListener<P2LBroadcastMessage> listener);
    /**
     * The given listener will receive all newly established connections.
     * @param listener listener to add
     */
    void addConnectionEstablishedListener(BiConsumer<P2LConnection, Integer> listener);
    /**
     * The given listener will receive all disconnected connections.
     * @param listener listener to add
     */
    void addConnectionDroppedListener(Consumer<P2LConnection> listener);

    /** Removes a previously assigned listener, by raw reference (i.e. ==)
     * @param listener listener to remove */
    void removeMessageListener(P2LMessageListener<ReceivedP2LMessage> listener);
    /** Removes a previously assigned listener, by raw reference (i.e. ==)
     * @param listener listener to remove */
    void removeBroadcastListener(P2LMessageListener<P2LBroadcastMessage> listener);
    /** Removes a previously assigned listener, by raw reference (i.e. ==)
     * @param listener listener to remove */
    void removeConnectionEstablishedListener(BiConsumer<P2LConnection, Integer> listener);
    /** Removes a previously assigned listener, by raw reference (i.e. ==)
     * @param listener listener to remove */
    void removeConnectionDroppedListener(Consumer<P2LConnection> listener);
    /** Trivial message listener - dual use for direct messages and broadcasts */
    interface P2LMessageListener<M extends P2LMessage> { void received(M message);}
}
