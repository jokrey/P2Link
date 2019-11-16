package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.stream.P2LInputStream;
import jokrey.utilities.network.link2peer.core.stream.P2LOutputStream;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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

    private P2Link selfLink;
    P2LNodeImpl(P2Link selfLink) throws IOException {
        this(selfLink, Integer.MAX_VALUE);
    }
    P2LNodeImpl(P2Link selfLink, int connectionLimit) throws IOException {
        this.selfLink = selfLink;
        this.connectionLimit = connectionLimit;

        incomingHandler = new IncomingHandler(this);

        new Thread(() -> {
            while(!incomingHandler.isClosed()) {
                long now = System.currentTimeMillis();
                try {
                    //todo - this in the future will also be required to keep alive nat holes - therefore it may need to be called more than the current every two minutes

                    List<P2Link> dormant = getDormantEstablishedConnections(now);
                    P2LFuture<Boolean>[] pingResults = new P2LFuture[dormant.size()];
                    for (int i = 0; i < dormant.size(); i++)
                        pingResults[i] = PingProtocol.asInitiator(this, dormant.get(i).getSocketAddress());
                    List<P2Link> retryableHistoricConnections = getRetryableHistoricConnections(now);
                    for(P2Link retryable:retryableHistoricConnections)
                        outgoingPool.execute(() -> EstablishConnectionProtocol.asInitiator(this, retryable, 1, P2LHeuristics.RETRY_HISTORIC_CONNECTION_TIMEOUT_MS)); //result does not matter - initiator will internally graduate a successful connection - and the timeout is much less than 10000

                    incomingHandler.internalMessageQueue.clean();
                    incomingHandler.receiptsQueue.clean();
                    incomingHandler.userBrdMessageQueue.clean();
                    incomingHandler.userMessageQueue.clean();
                    incomingHandler.broadcastState.clean(true);
                    incomingHandler.longMessageHandler.clean();

                    Thread.sleep(P2LHeuristics.MAIN_NODE_SLEEP_TIMEOUT_MS);

                    //the following uncommented code would also work - but the ping thing is cooler - maybe - at least it does not require reiterating established connections
//                    List<P2Link> dormantEstablishedAfterPing = getDormantEstablishedConnections(now); //using the old now here is correct
//                    for(P2Link stillDormant:dormantEstablishedAfterPing)
    //                    markBrokenConnection(stillDormant, true);
                    for(int i = 0; i < pingResults.length; i++) {
                        if (! (pingResults[i].isCompleted() && pingResults[i].get()))
                            markBrokenConnection(dormant.get(i), true);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }).start();
    }

    @Override public P2Link getSelfLink() { return selfLink; }
    @Override public void setSelfLink(P2Link selfLink) {
        this.selfLink = selfLink;
    }

    @Override public P2LFuture<P2Link> whoAmI(SocketAddress requestFrom) {
        return outgoingPool.execute(() -> WhoAmIProtocol.asInitiator(P2LNodeImpl.this, requestFrom));
    }

    @Override public void close() {
        disconnectFromAll();
        incomingHandler.close();
        outgoingPool.shutdown();
    }

    @Override public P2LFuture<Boolean> establishConnection(P2Link to) {
        return outgoingPool.execute(() -> isConnectedTo(to) || EstablishConnectionProtocol.asInitiator(P2LNodeImpl.this, to));
    }
    @Override public P2LFuture<Set<P2Link>> establishConnections(P2Link... addresses) {
        P2LThreadPool.Task[] tasks = new P2LThreadPool.Task[addresses.length];

        Set<P2Link> successes = ConcurrentHashMap.newKeySet(tasks.length);
        for(int i=0;i<addresses.length;i++) {
            P2Link address = addresses[i];
            tasks[i] = () -> {
                if(isConnectedTo(address) || EstablishConnectionProtocol.asInitiator(this, address))
                    successes.add(address);
            };
        }

        return outgoingPool.execute(tasks).toType(i -> successes);
    }
    @Override public void disconnectFrom(P2Link from) {
        DisconnectSingleConnectionProtocol.asInitiator(this, from);
    }

    @Override public List<P2Link> recursiveGarnerConnections(int newConnectionLimit, int newConnectionLimitPerRecursion, P2Link... setupLinks) {
        return GarnerConnectionsRecursivelyProtocol.recursiveGarnerConnections(this, newConnectionLimit, newConnectionLimitPerRecursion, Arrays.asList(setupLinks));
    }

    @Override public List<P2Link> queryKnownLinksOf(SocketAddress from) throws IOException {
        return RequestPeerLinksProtocol.asInitiator(this, from);
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

        //todo - is it really desirable to have packages be broken up THIS automatically???
        //todo    - like it is cool that breaking up packages does not make a difference, but... like it is so transparent it could lead to inefficiencies
        if(message.canBeSentInSinglePacket()) {
            System.out.println(getSelfLink()+" - P2LNodeImpl_sendInternalMessage - to = [" + to + "], message = [" + message + "]");
            incomingHandler.serverSocket.send(message.toPacket(to)); //since the server socket is bound to a port, said port will be included in the udp packet
        } else
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
    @Override public boolean sendMessageWithRetries(SocketAddress to, P2LMessage message, int attempts, int initialTimeout) {
        validateMsgIdNotInternal(message.header.getType());
        return sendInternalMessageWithRetries(message, to, attempts, initialTimeout);
    }
    @Override public P2LFuture<Boolean> sendInternalMessageWithReceipt(P2LMessage message, SocketAddress to) throws IOException {
        message.mutateToRequestReceipt();
        try {
            return P2LFuture.before(() ->
                    sendInternalMessage(message, to),
                    incomingHandler.receiptsQueue.receiptFutureFor(to, message.header.getType(), message.header.getConversationId()))
                    .toBooleanFuture(receipt -> receipt.validateIsReceiptFor(message));
        } catch (IOException t) {
            throw t;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new IOException(t.getClass()+" - "+t.getMessage());
        }
    }
    @Override public boolean sendInternalMessageWithRetries(P2LMessage message, SocketAddress to, int attempts, int initialTimeout) {
        boolean success = tryComplete(attempts, initialTimeout, () -> sendInternalMessageWithReceipt(message, to));
        if(!success)
            markBrokenConnection(toEstablished(to), true);
        return success;
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
//        System.out.println("incomingHandler.userBrdMessageQueue.debugString() = " + incomingHandler.userBrdMessageQueue.debugString());
        return incomingHandler.userBrdMessageQueue.futureFor(messageType);
    }
//    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(P2Link from, int messageType) {
//        validateMsgIdNotInternal(messageType);
//        return incomingHandler.userBrdMessageQueue.futureFor(from, messageType);
//    }
    @Override public P2LInputStream getInputStream(SocketAddress from, int messageType, int conversationId) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.streamMessageHandler.getInputStream(this, from, messageType, conversationId);
    }
    @Override public P2LOutputStream getOutputStream(SocketAddress to, int messageType, int conversationId) {
        validateMsgIdNotInternal(messageType);
        return incomingHandler.streamMessageHandler.getOutputStream(this, to, messageType, conversationId);
    }


    //CONNECTION KEEPER::
    /**
     * established connections
     * the value(the long) here indicates the last time a message was received from the connection - NOT THE LAST TIME A MESSAGE WAS SENT
     */
    private final ConcurrentHashMap<SocketAddress, P2LConnection> establishedConnections = new ConcurrentHashMap<>();
    /**
     * previously established connections
     * the value(the long) here indicates the time in ms since 1970, at which point a retry connection attempt should be made to the connection
     */
    private final ConcurrentHashMap<SocketAddress, HistoricConnection> historicConnections = new ConcurrentHashMap<>();
    private final int connectionLimit;
    @Override public boolean isConnectedTo(P2Link address) {
        //todo - this is not neccessarily correct - address can contain different socket address, but still be a link in a p2l connection...
        return address!=null && establishedConnections.containsKey(address.getSocketAddress());
    }
    @Override public boolean isConnectedTo(SocketAddress to) {
        return establishedConnections.containsKey(to);
    }
    @Override public P2Link toEstablished(SocketAddress address) {
        P2LConnection con = establishedConnections.get(address);
        return con==null?null:con.link;
    }
    @Override public Set<P2Link> getEstablishedConnections() {
        HashSet<P2Link> set = new HashSet<>(establishedConnections.size());
        for(P2LConnection con:establishedConnections.values())
            set.add(con.link);
        return set;
    }
    @Override public Set<P2Link> getPreviouslyEstablishedConnections() {
        HashSet<P2Link> set = new HashSet<>(historicConnections.size());
        for(HistoricConnection con:historicConnections.values())
            set.add(con.link);
        return set;
    }
    @Override public boolean connectionLimitReached() {
        return establishedConnections.size() >= connectionLimit;
    }
    @Override public int remainingNumberOfAllowedPeerConnections() {
        return connectionLimit - establishedConnections.size();
    }
    @Override public void graduateToEstablishedConnection(P2Link address, int conversationId) {
        //PROBLEM: if the initiator of a connection tells the other side its public ip(which differs from package.getAddress() - because we are in the same, non public NAT)
        //   if they lie about who they are, then we will attempt a connection to the public ip - and DDOS it inadvertently

        boolean previouslyConnected = establishedConnections.put(address.getSocketAddress(), new P2LConnection(address)) != null;
        historicConnections.remove(address.getSocketAddress());
        if(!previouslyConnected)
            notifyConnectionEstablished(address, conversationId);
    }
    @Override public void markBrokenConnection(P2Link address, boolean retry) {
        P2LConnection wasRemoved = establishedConnections.remove(address.getSocketAddress());
        if(wasRemoved==null) {
            System.err.println(address+" is not an established connection - could not mark as broken (already marked??)");
        } else {
            historicConnections.put(address.getSocketAddress(), new HistoricConnection(address));
            notifyConnectionDisconnected(address);
        }
    }
    @Override public void notifyPacketReceivedFrom(SocketAddress from) {
        //todo this operation may be to slow to compute EVERY TIME a packet is received - on the other hand..
        P2LConnection established = establishedConnections.get(from);
        if(established!=null) {
            established.notifyActivity();
        } else {
            HistoricConnection reEstablished = historicConnections.remove(from);
            if(reEstablished != null/* && retryStateOfHistoricConnection.r>0*/) //actively retrying IS NOT REQUIRED, the connection should be reestablished nonetheless
                graduateToEstablishedConnection(reEstablished.link, -1);//cool: auto remembering of correct(hopefully), link
        }
    }
    private List<P2Link> getDormantEstablishedConnections(long now) {
        ArrayList<P2Link> dormantConnections = new ArrayList<>(establishedConnections.size());
        for(Map.Entry<SocketAddress, P2LConnection> e:establishedConnections.entrySet())
            if(e.getValue().isDormant(now))
                dormantConnections.add(e.getValue().link);
        return dormantConnections;
    }
    /** Already sets the new retry time - so after using this method it is mandatory to actually retry the given connections  */
    private List<P2Link> getRetryableHistoricConnections(long now) {
        ArrayList<P2Link> retryableHistoricConnections = new ArrayList<>(Math.min(16, historicConnections.size()));
        for(Map.Entry<SocketAddress, HistoricConnection> e:historicConnections.entrySet())
            if(e.getValue().retryNow(now))
                retryableHistoricConnections.add(e.getValue().link);
        return retryableHistoricConnections;
    }

    @Override public P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks) {
        return outgoingPool.execute(tasks);
    }





    //LISTENERS:
    private final ArrayList<P2LMessageListener> individualMessageListeners = new ArrayList<>();
    private final ArrayList<P2LMessageListener> broadcastMessageListeners = new ArrayList<>();
    private final ArrayList<BiConsumer<P2Link, Integer>> newConnectionEstablishedListeners = new ArrayList<>();
    private final ArrayList<Consumer<P2Link>> connectionDisconnectedListeners = new ArrayList<>();
    @Override public void addMessageListener(P2LMessageListener listener) { individualMessageListeners.add(listener); }
    @Override public void addBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.add(listener); }
    @Override public void addConnectionEstablishedListener(BiConsumer<P2Link, Integer> listener) { newConnectionEstablishedListeners.add(listener); }
    @Override public void addConnectionDroppedListener(Consumer<P2Link> listener) { connectionDisconnectedListeners.add(listener); }
    @Override public void removeMessageListener(P2LMessageListener listener) { individualMessageListeners.remove(listener); }
    @Override public void removeBroadcastListener(P2LMessageListener listener) { broadcastMessageListeners.remove(listener); }
    @Override public void removeConnectionEstablishedListener(BiConsumer<P2Link, Integer> listener) { newConnectionEstablishedListeners.remove(listener); }
    @Override public void removeConnectionDroppedListener(Consumer<P2Link> listener) { connectionDisconnectedListeners.remove(listener); }

    @Override public void notifyUserBroadcastMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyUserMessageReceived(P2LMessage message) {
        for (P2LMessageListener l : individualMessageListeners) { l.received(message); }
    }

    private void notifyConnectionEstablished(P2Link newAddress, int conversationId) {
        for (BiConsumer<P2Link, Integer> l : newConnectionEstablishedListeners) { l.accept(newAddress, conversationId); }
    }
    private void notifyConnectionDisconnected(P2Link newAddress) {
        for (Consumer<P2Link> l : connectionDisconnectedListeners) { l.accept(newAddress); }
    }


    private AtomicInteger runningConversationId = new AtomicInteger(NO_CONVERSATION_ID + 1);
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



    static class P2LConnection {
        P2Link link;
        public P2LConnection(P2Link link) {this.link=link;}
        long lastPacketReceived = System.currentTimeMillis();
        boolean isDormant(long now) {
            return (now - lastPacketReceived) > P2LHeuristics.ESTABLISHED_CONNECTION_IS_DORMANT_THRESHOLD_MS;
        }
        void notifyActivity() {lastPacketReceived = System.currentTimeMillis();}

        @Override
        public String toString() {
            return "P2LConnection{" +
                    "link=" + link +
                    ", lastPacketReceived=" + lastPacketReceived +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            P2LConnection that = (P2LConnection) o;
            return lastPacketReceived == that.lastPacketReceived &&
                    Objects.equals(link, that.link);
        }

        @Override
        public int hashCode() {
            return Objects.hash(link, lastPacketReceived);
        }
    }
    static class HistoricConnection {
        P2Link link;
        public HistoricConnection(P2Link link) {
            this.link=link;
            if(link.getSocketAddress()==null)
                throw new NullPointerException("otherwise we would have a problem on retry");
        }
        long nextAttemptAt = System.currentTimeMillis();
        int numberOfAttemptsMade;

        boolean retryNow(long now) {
            if(nextAttemptAt <= now) {
                long newTime = (long) (nextAttemptAt + P2LHeuristics.ORIGINAL_RETRY_HISTORIC_TIMEOUT_MS * Math.pow(2, numberOfAttemptsMade));
                if (newTime < nextAttemptAt) {
                    nextAttemptAt = Long.MAX_VALUE;
                } else {
                    nextAttemptAt = newTime;
                    numberOfAttemptsMade++;
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HistoricConnection that = (HistoricConnection) o;
            return nextAttemptAt == that.nextAttemptAt &&
                    numberOfAttemptsMade == that.numberOfAttemptsMade &&
                    Objects.equals(link, that.link);
        }

        @Override
        public int hashCode() {
            return Objects.hash(link, nextAttemptAt, numberOfAttemptsMade);
        }

        @Override
        public String toString() {
            return "HistoricConnection{" +
                    "link=" + link +
                    ", nextAttemptAt=" + nextAttemptAt +
                    ", numberOfAttemptsMade=" + numberOfAttemptsMade +
                    '}';
        }
    }
}