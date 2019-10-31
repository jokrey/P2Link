package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.validateMsgIdNotInternal;

/**
 *
 * NOT THREAD SAFE
 *
 * @author jokrey
 */
final class P2LNodeImpl implements P2LNode, P2LNodeInternal {
    private final IncomingHandler incomingHandler;
    private final P2LThreadPool outgoingPool = new P2LThreadPool(4, 64);

    private final int port;
    P2LNodeImpl(int port) throws IOException {
        this(port, Integer.MAX_VALUE);
    }
    P2LNodeImpl(int port, int connectionLimit) throws IOException {
        this.port = port;
        this.connectionLimit = connectionLimit;

        incomingHandler = new IncomingHandler(this);

        new Thread(() -> {
            long lastPing = System.currentTimeMillis();
            while(!incomingHandler.isClosed()) {
                long startTime = System.currentTimeMillis();
                try {

                    //todo test ping protocol - reset last ping after
                    SocketAddress[] origEstablished = null;
                    ArrayList<P2LFuture<SocketAddress>> pingedList = null;
                    if((System.currentTimeMillis()-lastPing)/1e3 > 2*60) {
                        origEstablished = getEstablishedConnections().toArray(new SocketAddress[0]);
                        pingedList = new ArrayList<>(origEstablished.length);
                        for(SocketAddress established:origEstablished)
                            pingedList.add(PingProtocol.asInitiator(this, established));
                    }

                    //maybe retry historic connections (based on their timeout value)

                    //todo test clean up (verify it does what it is supposed to...)
                    incomingHandler.internalMessageQueue.cleanExpiredMessages();
                    incomingHandler.receiptsQueue.cleanExpiredMessages();
                    incomingHandler.userBrdMessageQueue.cleanExpiredMessages();
                    incomingHandler.userMessageQueue.cleanExpiredMessages();
                    incomingHandler.broadcastState.clean();

                    if(pingedList != null) {
                        Collection<SocketAddress> stillEstablishedConnections = P2LFuture.waitForEm(pingedList, 10000);

                        for(SocketAddress established:origEstablished)
                            if(!stillEstablishedConnections.contains(established))
                                markBrokenConnection(established, true);

                        lastPing = System.currentTimeMillis();
                    }
                    long elapsed = System.currentTimeMillis() - startTime;
                    Thread.sleep(Math.max(10, 10000 - elapsed));
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }).start();
    }

    @Override public int getPort() { return port; }

    @Override public void close() {
        disconnectFromAll();
        incomingHandler.close();
        outgoingPool.shutdown();
    }

    @Override public P2LFuture<Boolean> establishConnection(SocketAddress to) {
        return outgoingPool.execute(() -> isConnectedTo(to) || EstablishSingleConnectionProtocol.asInitiator(P2LNodeImpl.this, to));
    }
    @Override public P2LFuture<Set<SocketAddress>> establishConnections(SocketAddress... addresses) {
        P2LThreadPool.Task[] tasks = new P2LThreadPool.Task[addresses.length];

        Set<SocketAddress> successes = ConcurrentHashMap.newKeySet(tasks.length);
        for(int i=0;i<addresses.length;i++) {
            SocketAddress address = addresses[i];
            tasks[i] = () -> {
                if(!isConnectedTo(address))
                    EstablishSingleConnectionProtocol.asInitiator(this, address);
                successes.add(address);
            };
        }

        return outgoingPool.execute(tasks).toType(i -> successes);
    }
    @Override public void disconnectFrom(SocketAddress from) {
        DisconnectSingleConnectionProtocol.asInitiator(this, from);
    }

    @Override public List<SocketAddress> recursiveGarnerConnections(int newConnectionLimit, SocketAddress... setupLinks) {
        return GarnerConnectionsRecursivelyProtocol.recursiveGarnerConnections(this, newConnectionLimit, Integer.MAX_VALUE, Arrays.asList(setupLinks));
    }

    @Override public P2LFuture<Integer> sendBroadcastWithReceipts(P2LMessage message) {
        if(message.sender == null) throw new IllegalArgumentException("sender of message has to be attached in broadcasts");
        validateMsgIdNotInternal(message.type);

        incomingHandler.broadcastState.markAsKnown(message.getContentHash());

        return BroadcastMessageProtocol.relayBroadcast(this, message);
    }








    //DIRECT MESSAGING:
    @Override public void sendInternalMessage(P2LMessage message, SocketAddress to) throws IOException {
        if(message.sender != null) throw new IllegalArgumentException("sender of message has to be this null and will be automatically set by the sender");
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
    @Override public P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage origMessage, SocketAddress to) throws IOException {
        P2LMessage message = origMessage.mutateToRequestReceipt();
        sendInternalMessage(message, to);
        return incomingHandler.receiptsQueue.receiptFutureFor(to, message.type, message.conversationId).toBooleanFuture(receipt ->
                receipt.validateIsReceiptFor(message));
    }
    @Override public void sendInternalMessageBlocking(P2LMessage message, SocketAddress to, int retries, int initialTimeout) throws IOException {
        try {
            tryComplete(retries, initialTimeout, () -> sendInternalMessageWithReceipt(message, to));
        } catch(IOException e) {
            markBrokenConnection(to, true);
            throw e;
        }
    }

    @Override public P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int messageType) {
        return incomingHandler.internalMessageQueue.futureFor(from, messageType);
    }
    @Override public P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int messageType, int conversationId) {
        return incomingHandler.internalMessageQueue.futureFor(from, messageType, conversationId);
    }
    @Override public P2LFuture<P2LMessage> expectMessage(int messageType) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.userMessageQueue.futureFor(messageType);
    }
    public P2LFuture<P2LMessage> expectMessage(SocketAddress from, int messageType) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.userMessageQueue.futureFor(from, messageType);
    }
    public P2LFuture<P2LMessage> expectMessage(SocketAddress from, int messageType, int conversationId) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.userMessageQueue.futureFor(from, messageType, conversationId);
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(int messageType) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.userBrdMessageQueue.futureFor(messageType);
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(String from, int messageType) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.userBrdMessageQueue.futureFor(from, messageType);
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
    @Override public Set<SocketAddress> getPreviouslyEstablishedConnections() {
        return historicConnections.keySet();
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
        notifyConnectionEstablished(address);
    }
    @Override public void markBrokenConnection(SocketAddress address, boolean retry) {
        boolean wasRemoved = establishedConnections.remove(address);
        if(!wasRemoved) {
            System.err.println(address+" is not an established connection - could not mark as broken (already marked??)");
        } else {
            historicConnections.put(address, retry ? System.currentTimeMillis() + 1000 * 60 * 2 : Long.MAX_VALUE);
            notifyConnectionDisconnected(address);
        }
    }
    @Override public P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks) {
        return outgoingPool.execute(tasks);
    }







    //LISTENERS:
    private final ArrayList<P2LMessageListener> individualMessageListeners = new ArrayList<>();
    private final ArrayList<P2LMessageListener> broadcastMessageListeners = new ArrayList<>();
    private final ArrayList<Consumer<SocketAddress>> newConnectionEstablishedListeners = new ArrayList<>();
    private final ArrayList<Consumer<SocketAddress>> connectionDisconnectedListeners = new ArrayList<>();
    @Override public void addMessageListener(P2LMessageListener listener) { individualMessageListeners.add(listener); }
    @Override public void addBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.add(listener); }
    @Override public void addConnectionEstablishedListener(Consumer<SocketAddress> listener) { newConnectionEstablishedListeners.add(listener); }
    @Override public void addConnectionDisconnectedListener(Consumer<SocketAddress> listener) { connectionDisconnectedListeners.add(listener); }
    @Override public void removeMessageListener(P2LMessageListener listener) { individualMessageListeners.remove(listener); }
    @Override public void removeBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.remove(listener); }
    @Override public void removeConnectionEstablishedListener(Consumer<SocketAddress> listener) { newConnectionEstablishedListeners.remove(listener); }
    @Override public void removeConnectionDisconnectedListener(Consumer<SocketAddress> listener) { connectionDisconnectedListeners.remove(listener); }
    @Override public void notifyBroadcastMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : individualMessageListeners) { l.received(message); }
    }

    private void notifyConnectionEstablished(SocketAddress newAddress) {
        for (Consumer<SocketAddress> l : newConnectionEstablishedListeners) { l.accept(newAddress); }
    }
    private void notifyConnectionDisconnected(SocketAddress newAddress) {
        for (Consumer<SocketAddress> l : connectionDisconnectedListeners) { l.accept(newAddress); }
    }


    private AtomicInteger runningConversationId = new AtomicInteger(1);
    @Override public int createUniqueConversationId() {
        int uniqueConversationId;
        do {
            uniqueConversationId = runningConversationId.getAndIncrement();
        } while(uniqueConversationId==NO_CONVERSATION_ID); //race condition does not matter here - maybe a number is skipped, but it is still unique
        return uniqueConversationId;//will eventually overflow - but by then the conversation has likely ended
                                       //does not need to be unique between nodes - because it is always in combination with from(sender)+type
                                       //potential problem:
    }
}
