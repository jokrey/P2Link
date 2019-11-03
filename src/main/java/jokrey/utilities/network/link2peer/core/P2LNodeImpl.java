package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.validateMsgIdNotInternal;

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
            while(!incomingHandler.isClosed()) {
                long now = System.currentTimeMillis();
                try {
                    List<SocketAddress> dormantEstablishedBeforePing = getDormantEstablishedConnections(now);
                    for(SocketAddress dormant:dormantEstablishedBeforePing)
                        PingProtocol.asInitiator(this, dormant);
                    List<SocketAddress> retryableHistoricConnections = getRetryableHistoricConnections();
                    for(SocketAddress retryable:retryableHistoricConnections)
                        outgoingPool.execute(() -> EstablishSingleConnectionProtocol.asInitiator(this, retryable, 1, P2LHeuristics.RETRY_HISTORIC_CONNECTION_TIMEOUT_MS)); //result does not matter - initiator will internally graduate a successful connection - and the timeout is much less than 10000

                    incomingHandler.internalMessageQueue.cleanExpiredMessages();
                    incomingHandler.receiptsQueue.cleanExpiredMessages();
                    incomingHandler.userBrdMessageQueue.cleanExpiredMessages();
                    incomingHandler.userMessageQueue.cleanExpiredMessages();
                    incomingHandler.broadcastState.clean(true);
                    incomingHandler.longMessageHandler.clean();

                    Thread.sleep(P2LHeuristics.MAIN_NODE_SLEEP_TIMEOUT_MS);

                    List<SocketAddress> dormantEstablishedAfterPing = getDormantEstablishedConnections(now);
                    for(SocketAddress stillDormant:dormantEstablishedAfterPing)
                        markBrokenConnection(stillDormant, true);
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
        if(message.header.getSender() == null) throw new IllegalArgumentException("sender of message has to be attached in broadcasts");
        validateMsgIdNotInternal(message.header.getType());

        incomingHandler.broadcastState.markAsKnown(message.getContentHash());

        return BroadcastMessageProtocol.relayBroadcast(this, message);
    }








    //DIRECT MESSAGING:
    @Override public void sendInternalMessage(P2LMessage message, SocketAddress to) throws IOException {
        if(message.header.getSender() != null) throw new IllegalArgumentException("sender of message has to be this null and will be automatically set by the sender");
//        System.out.println(getPort()+" - P2LNodeImpl_sendInternalMessage - to = [" + to + "], message = [" + message + "]");

        //todo - is it really desirable to have packages be broken up THIS automatically???
        //todo    - like it is cool that breaking up packages does not make a difference, but... like it is so transparent it could lead to inefficiencies
        if(message.canBeSentInSinglePacket())
            incomingHandler.serverSocket.send(message.toPacket(to)); //since the server socket is bound to a port, said port will be included in the udp packet
        else
            incomingHandler.longMessageHandler.send(this, message, to);
    }
    @Override public void sendMessage(SocketAddress to, P2LMessage message) throws IOException {
        validateMsgIdNotInternal(message.header.getType());
        sendInternalMessage(message, to);
    }
    @Override public P2LFuture<Boolean> sendMessageWithReceipt(SocketAddress to, P2LMessage message) throws IOException {
        validateMsgIdNotInternal(message.header.getType());
        return sendInternalMessageWithReceipt(message, to);
    }
    @Override public void sendMessageBlocking(SocketAddress to, P2LMessage message, int attempts, int initialTimeout) throws IOException {
        validateMsgIdNotInternal(message.header.getType());
        sendInternalMessageBlocking(message, to, attempts, initialTimeout);
    }
    @Override public P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage message, SocketAddress to) throws IOException {
        message.mutateToRequestReceipt();
        try {
            return incomingHandler.receiptsQueue.receiptFutureFor(to, message.header.getType(), message.header.getConversationId())
                    .nowOrCancel(() -> sendInternalMessage(message, to)) //weird syntax, but we have to make sure we already wait for the receipt when we send the message (receipt expire instantly
                    .toBooleanFuture(receipt -> {
                        return receipt.validateIsReceiptFor(message);
                    });
        } catch (IOException t) {
            throw t;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new IOException(t.getClass()+" - "+t.getMessage());
        }
    }
    @Override public void sendInternalMessageBlocking(P2LMessage message, SocketAddress to, int attempts, int initialTimeout) throws IOException {
        try {
            tryComplete(attempts, initialTimeout, () -> sendInternalMessageWithReceipt(message, to));
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
    @Override public InputStream getInputStream(SocketAddress from, int messageType, int conversationId) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.streamMessageHandler.getInputStream(from, messageType, conversationId);
    }





    //CONNECTION KEEPER::
    /**
     * established connections
     * the value(the long) here indicates the last time a message was received from the connection - NOT THE LAST TIME A MESSAGE WAS SENT
     */
    private final ConcurrentHashMap<SocketAddress, Long> establishedConnections = new ConcurrentHashMap<>();
    /**
     * previously established connections
     * the value(the long) here indicates the time in ms since 1970, at which point a retry connection attempt should be made to the connection
     */
    private final ConcurrentHashMap<SocketAddress, Pair<Long, Integer>> historicConnections = new ConcurrentHashMap<>();
    private final int connectionLimit;
    @Override public boolean isConnectedTo(SocketAddress address) {
        return establishedConnections.containsKey(address);
    }
    @Override public Set<SocketAddress> getEstablishedConnections() {
        return establishedConnections.keySet();
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
        establishedConnections.put(address, System.currentTimeMillis());
        historicConnections.remove(address);
        notifyConnectionEstablished(address);
    }
    @Override public void markBrokenConnection(SocketAddress address, boolean retry) {
        Long wasRemoved = establishedConnections.remove(address);
        if(wasRemoved==null) {
            System.err.println(address+" is not an established connection - could not mark as broken (already marked??)");
        } else {
            historicConnections.put(address, retry ? new Pair<>(System.currentTimeMillis() + 1000 * 60 * 2, 0) : new Pair<>(Long.MAX_VALUE, -1));
            notifyConnectionDisconnected(address);
        }
    }
    @Override public void notifyPacketReceivedFrom(SocketAddress from) {
        boolean isEstablished = establishedConnections.computeIfPresent(from, (f,p)->System.currentTimeMillis()) != null;
        Pair<Long, Integer> retryStateOfHistoricConnection = isEstablished||connectionLimitReached()?null:historicConnections.get(from);
        if(retryStateOfHistoricConnection != null/* && retryStateOfHistoricConnection.r>0*/) //not actively retrying does not mean the connection should not be reestablished
            graduateToEstablishedConnection(from);
    }
    private List<SocketAddress> getDormantEstablishedConnections(long now) {
        ArrayList<SocketAddress> dormantConnections = new ArrayList<>(establishedConnections.size());
        for(Map.Entry<SocketAddress, Long> e:establishedConnections.entrySet())
            if((now - e.getValue()) > P2LHeuristics.ESTABLISHED_CONNECTION_IS_DORMANT_THRESHOLD_MS)
                dormantConnections.add(e.getKey());
        return dormantConnections;
    }
    /** Already sets the new retry time - so after using this method it is mandatory to actually retry the given connections  */
    private List<SocketAddress> getRetryableHistoricConnections() {
        ArrayList<SocketAddress> retryableHistoricConnections = new ArrayList<>(Math.min(16, historicConnections.size()));
        for(Map.Entry<SocketAddress, Pair<Long, Integer>> e:historicConnections.entrySet()) {
            long nextRetry = e.getValue().l;
            int totalNumberOfPreviousRetries = e.getValue().r;
            if(nextRetry <= System.currentTimeMillis()) {
                e.setValue(new Pair<>(Math.min(Long.MAX_VALUE, (long) (nextRetry + P2LHeuristics.ORIGINAL_RETRY_HISTORIC_TIMEOUT_MS * Math.pow(2, totalNumberOfPreviousRetries))), totalNumberOfPreviousRetries+1));

                retryableHistoricConnections.add(e.getKey());
            }
        }
        return retryableHistoricConnections;
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

    @Override public void notifyUserBroadcastMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyUserMessageReceived(P2LMessage message) {
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


    @Override public void printDebugInformation() {
        System.out.println("----- DEBUG INFORMATION -----");
        System.out.println("isClosed = "+incomingHandler.isClosed());
        System.out.println("connectionLimit = " + connectionLimit);
        System.out.println("establishedConnections("+establishedConnections.size()+") = " + establishedConnections);
        System.out.println("historicConnections("+historicConnections.size()+") = " + historicConnections);
        System.out.println("incomingHandler.broadcastState = " + incomingHandler.broadcastState.debugString());
        System.out.println("incomingHandler.internalMessageQueue.debugString() = " + incomingHandler.internalMessageQueue.debugString());
        System.out.println("incomingHandler.receiptsQueue.debugString() = " + incomingHandler.receiptsQueue.debugString());
        System.out.println("incomingHandler.userMessageQueue.debugString() = " + incomingHandler.userMessageQueue.debugString());
        System.out.println("incomingHandler.userBrdMessageQueue.debugString() = " + incomingHandler.userBrdMessageQueue.debugString());
        System.out.println("incomingHandler.longMessageHandler.debugString() = " + incomingHandler.longMessageHandler.debugString());
        System.out.println("incomingHandler.handleReceivedMessagesPool = " + incomingHandler.handleReceivedMessagesPool.debugString());
        System.out.println("outgoingPool = " + outgoingPool.debugString());
        System.out.println("individualMessageListeners = " + individualMessageListeners);
        System.out.println("broadcastMessageListeners = " + broadcastMessageListeners);
        System.out.println("newConnectionEstablishedListeners = " + newConnectionEstablishedListeners);
        System.out.println("connectionDisconnectedListeners = " + connectionDisconnectedListeners);
        System.out.println("runningConversationId = " + runningConversationId.get());
        System.out.println("-END- DEBUG INFORMATION -END-");
    }


}
