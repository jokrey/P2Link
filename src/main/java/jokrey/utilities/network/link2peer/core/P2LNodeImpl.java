package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.core.OutgoingHandler.Task;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.validateMsgIdNotInternal;

/**
 *
 * NOT THREAD SAFE
 *
 * @author jokrey
 */
final class P2LNodeImpl implements P2LNode, P2LNodeInternal {
    private final IncomingHandler incomingHandler;
    private final OutgoingHandler outgoingHandler;

    public final int port;
    P2LNodeImpl(int port) throws IOException {
        this(port, Integer.MAX_VALUE);
    }
    P2LNodeImpl(int port, int connectionLimit) throws IOException {
        this.port = port;
        this.connectionLimit = connectionLimit;

        incomingHandler = new IncomingHandler(this);
        outgoingHandler = new OutgoingHandler();

        new Thread(() -> {
            while(true) {
                try {

                    //todo - ping protocol to clean up established connections

                    incomingHandler.internalMessageQueue.cleanExpiredMessages();
                    incomingHandler.receiptsQueue.cleanExpiredMessages();
                    incomingHandler.userBrdMessageQueue.cleanExpiredMessages();
                    incomingHandler.userMessageQueue.cleanExpiredMessages();
                    incomingHandler.broadcastState.clean();

                    Thread.sleep(5000);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }).start();
    }

    @Override public int getPort() { return port; }

    @Override public P2LFuture<Set<SocketAddress>> establishConnections(SocketAddress... addresses) {
        Task[] tasks = new Task[addresses.length];

        Set<SocketAddress> successes = ConcurrentHashMap.newKeySet(tasks.length);
        for(int i=0;i<addresses.length;i++) {
            SocketAddress address = addresses[i];
            tasks[i] = () -> {
                try {
                    if(!isConnectedTo(address)) {
                        EstablishSingleConnectionProtocol.asInitiator(this, address);
                    }
                    successes.add(address);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return false;
            };
        }

        return outgoingHandler.executeAll(tasks).toType(p -> successes);
    }
    @Override public void disconnectFrom(SocketAddress address) {
        DisconnectSingleConnectionProtocol.asInitiator(this, address);
    }

    @Override public List<SocketAddress> recursiveGarnerConnections(int newConnectionLimit, SocketAddress... setupLinks) {
        return GarnerConnectionsRecursivelyProtocol.recursiveGarnerConnections(this, newConnectionLimit, Integer.MAX_VALUE, Arrays.asList(setupLinks));
    }

    @Override public P2LFuture<Pair<Integer, Integer>> sendBroadcastWithReceipts(P2LMessage message) {
        if(message.sender == null) throw new IllegalArgumentException("sender of message has to be attached in broadcasts");
        validateMsgIdNotInternal(message.type);

        SocketAddress[] originallyActivePeers = establishedConnections.toArray(new SocketAddress[0]);
        if(originallyActivePeers.length == 0)
            return new P2LFuture<>(new Pair<>(0 , 0));

        incomingHandler.broadcastState.markAsKnown(message.getHash());

        Task[] tasks = new Task[originallyActivePeers.length];
        for(int i=0;i<tasks.length;i++) {
            SocketAddress connection = originallyActivePeers[i];
            tasks[i] = () -> {
                BroadcastMessageProtocol.asInitiator(this, message, connection);
                return true;
            };
        }

        return outgoingHandler.executeAll(tasks);
    }








    //DIRECT MESSAGING:
    @Override public void sendInternalMessage(P2LMessage message, SocketAddress to) throws IOException {
        if(message.sender != null) throw new IllegalArgumentException("sender of message has to be this node's link or null");
        incomingHandler.serverSocket.send(message.getPacket(to)); //since the server socket is bound to a port, said port will be included in the udp packet
//        System.out.println(getPort()+" - P2LNodeImpl_sendInternalMessage - to = [" + to + "], message = [" + message + "]");
    }
    @Override public void sendMessage(SocketAddress to, P2LMessage message) throws IOException {
        validateMsgIdNotInternal(message.type);
        sendInternalMessage(message, to);
    }
    @Override public P2LFuture<Boolean> sendMessageWithReceipt(SocketAddress to, P2LMessage message) throws IOException {
        validateMsgIdNotInternal(message.type);
        return sendInternalMessageWithReceipt(message, to);
    }
    @Override public void sendMessageBlocking(SocketAddress to, P2LMessage message, int retries, int initialTimeout) throws IOException {
        validateMsgIdNotInternal(message.type);
        sendInternalMessageBlocking(message, to, retries, initialTimeout);
    }
    @Override public P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage origMessage, SocketAddress receiver) throws IOException {
        P2LMessage message = origMessage.mutateToRequestReceipt();
        sendInternalMessage(message, receiver);
        return incomingHandler.receiptsQueue.receiptFutureFor(receiver, message.type, message.conversationId).toBooleanFuture(receipt ->
                receipt.validateIsReceiptFor(message));
    }
    @Override public void sendInternalMessageBlocking(P2LMessage message, SocketAddress receiver, int retries, int initialTimeout) throws IOException {
        try {
            tryComplete(retries, initialTimeout, () -> {
//                System.out.println(getPort()+" - "+Thread.currentThread().getId()+" - try send blocking: "+message);
                return sendInternalMessageWithReceipt(message, receiver);
            });
        } catch(IOException e) {
            markBrokenConnection(receiver, true);
            throw e;
        }
    }

    @Override public P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId) {
        return incomingHandler.internalMessageQueue.futureFor(from, msgId);
    }
    @Override public P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int msgId, int conversationId) {
        return incomingHandler.internalMessageQueue.futureFor(from, msgId, conversationId);
    }
    @Override public P2LFuture<P2LMessage> expectMessage(int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userMessageQueue.futureFor(msgId);
    }
    public P2LFuture<P2LMessage> expectMessage(SocketAddress from, int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userMessageQueue.futureFor(from, msgId);
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userBrdMessageQueue.futureFor(msgId);
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(String from, int msgId) {
        validateMsgIdNotInternal(msgId);
        return incomingHandler.userBrdMessageQueue.futureFor(from, msgId);
    }






    //CONNECTION KEEPER::
    private final Set<SocketAddress> establishedConnections = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<SocketAddress, Long> historicConnections = new ConcurrentHashMap<>(); //previously established connections
    private final int connectionLimit;
    @Override public boolean isConnectedTo(SocketAddress address) {
        return establishedConnections.contains(address);
    }
    @Override public Set<SocketAddress> getEstablishedConnections() {
        return establishedConnections;
    }
    @Override public boolean connectionLimitReached() {
        return establishedConnections.size() >= connectionLimit;
    }
    @Override public int remainingNumberOfAllowedPeerConnections() {
        return connectionLimit - establishedConnections.size();
    }

    @Override public void graduateToEstablishedConnection(SocketAddress address) {
        establishedConnections.add(address);
        historicConnections.remove(address);
        notifyNewConnection(address);
    }
    @Override public void markBrokenConnection(SocketAddress address, boolean retry) {
        boolean wasRemoved = establishedConnections.remove(address);
        if(!wasRemoved) {
            System.err.println(address+" is not an established connection - could not mark as broken (already marked??)");
        } else
            historicConnections.put(address, System.currentTimeMillis());
    }
    @Override public P2LFuture<Pair<Integer, Integer>> executeAllOnSendThreadPool(Task... tasks) {
        return outgoingHandler.executeAll(tasks);
    }







    //LISTENERS:
    private final ArrayList<P2LMessageListener> individualMessageListeners = new ArrayList<>();
    private final ArrayList<P2LMessageListener> broadcastMessageListeners = new ArrayList<>();
    private final ArrayList<Consumer<SocketAddress>> newConnectionEstablishedListeners = new ArrayList<>();
    @Override public void addMessageListener(P2LMessageListener listener) { individualMessageListeners.add(listener); }
    @Override public void addBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.add(listener); }
    @Override public void addNewConnectionListener(Consumer<SocketAddress> listener) { newConnectionEstablishedListeners.add(listener); }
    @Override public void removeMessageListener(P2LMessageListener listener) { individualMessageListeners.remove(listener); }
    @Override public void removeBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.remove(listener); }
    @Override public void removeNewConnectionListener(Consumer<SocketAddress> listener) { newConnectionEstablishedListeners.add(listener); }
    @Override public void notifyBroadcastMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : individualMessageListeners) { l.received(message); }
    }

    private void notifyNewConnection(SocketAddress newAddress) {
        for (Consumer<SocketAddress> l : newConnectionEstablishedListeners) { l.accept(newAddress); }
    }


    private AtomicInteger runningConversationId = new AtomicInteger(1);
    @Override public int createUniqueConversationId() {
        int uniqueConvId;
        do {
            uniqueConvId = runningConversationId.getAndIncrement();
        } while(uniqueConvId==NO_CONVERSATION_ID); //race condition does not matter here - maybe a number is skipped, but it is still unique
        return uniqueConvId;//will eventually overflow - but by then the conversation has likely ended
                                       //does not need to be unique between nodes - because it is always in combination with from(sender)+type
                                       //potential problem:
    }
}
