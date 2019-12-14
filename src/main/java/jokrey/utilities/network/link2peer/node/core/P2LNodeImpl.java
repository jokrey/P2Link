package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.protocols.*;
import jokrey.utilities.network.link2peer.node.stream.*;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.validateMsgTypeNotInternal;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.NO_CONVERSATION_ID;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

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

                    incomingHandler.messageQueue.clean();
                    incomingHandler.brdMessageQueue.clean();
                    incomingHandler.broadcastState.clean(true);
                    incomingHandler.longMessageHandler.clean();
                    incomingHandler.conversationMessageHandler.clean();

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
        validateMsgTypeNotInternal(message.header.getType());

        incomingHandler.broadcastState.markAsKnown(message.getContentHash());

        return BroadcastMessageProtocol.relayBroadcast(this, message);
    }


    @Override public void registerInternalConversationFor(int type, ConversationAnswererChangeThisName handler) {
        incomingHandler.conversationMessageHandler.registerConversationFor(type, handler);
    }

    @Override public P2LConversationImpl internalConvo(int type, int conversationId, SocketAddress to) {
        return new P2LConversationImpl(this, incomingHandler.conversationMessageHandler.conversationQueue,
                establishedConnections.get(to), to,
                toShort(conversationId), toShort(type));
    }

    //DIRECT MESSAGING:
    @Override public void sendInternalMessage(SocketAddress to, P2LMessage message) throws IOException {
        if(message.header.getSender() != null) throw new IllegalArgumentException("sender of message has to be this null and will be automatically set by the sender");

        if(message.canBeSentInSinglePacket()) {
            if(DebugStats.MSG_PRINTS_ACTIVE)
                System.out.println(getSelfLink()+" - sendInternalMessage - to = [" + to + "], message = " + message);
            incomingHandler.serverSocket.send(message.toPacket(to));
        } else {
            //todo - is it really desirable to have packages be broken up THIS automatically???
            //todo    - like it is cool that breaking up packages does not make a difference, but... like it is so transparent it could lead to inefficiencies
//            throw new IllegalStateException(message.size+"");
            System.out.println("sendLong(being broken up) - message = " + message + ", to = " + to);
            incomingHandler.longMessageHandler.send(this, message, to);
        }
    }
    @Override public P2LFuture<Boolean> sendInternalMessageWithReceipt(SocketAddress to, P2LMessage message) throws IOException {
        message.mutateToRequestReceipt();
        try {
            return P2LFuture.before(() ->
                    sendInternalMessage(to, message),
                    incomingHandler.messageQueue.receiptFutureFor(to, message.header.getType(), message.header.getConversationId()))
                    .toBooleanFuture(receipt -> receipt.validateIsReceiptFor(message));
        } catch (IOException t) {
            throw t;
        } catch (Throwable t) {
            t.printStackTrace();
            throw new IOException(t.getClass()+" - "+t.getMessage());
        }
    }

    @Override public boolean sendInternalMessageWithRetries(SocketAddress to, P2LMessage message, int attempts) {
        P2LConnection con = establishedConnections.get(to);
        if(con == null) throw new IllegalArgumentException("to("+to+") has to be an established connection - to automatically determine the required values");
        return sendInternalMessageWithRetries(message, to, attempts, (int) (con.avRTT*1.1));
    }

    @Override public P2LFuture<P2LMessage> expectMessage(int messageType) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.messageQueue.futureFor(toShort(messageType));
    }
    @Override public P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int messageType) {
        return incomingHandler.messageQueue.futureFor(from, toShort(messageType));
    }
    @Override public P2LFuture<P2LMessage> expectInternalMessage(SocketAddress from, int messageType, int conversationId) {
        return incomingHandler.messageQueue.futureFor(from, toShort(messageType), toShort(conversationId));
    }
    @Override public P2LFuture<P2LMessage> expectBroadcastMessage(int messageType) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.brdMessageQueue.futureFor(toShort(messageType));
    }
    @Override public P2LOrderedInputStream createInputStream(SocketAddress from, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createInputStream(this, from, establishedConnections.get(from), toShort(messageType), toShort(conversationId));
    }
    @Override public boolean registerCustomInputStream(SocketAddress from, int messageType, int conversationId, P2LInputStream inputStream) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createCustomInputStream(from, toShort(messageType), toShort(conversationId), inputStream);
    }
    @Override public P2LOrderedOutputStream createOutputStream(SocketAddress to, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createOutputStream(this, to, establishedConnections.get(to), toShort(messageType), toShort(conversationId));
    }
    @Override public boolean registerCustomOutputStream(SocketAddress to, int messageType, int conversationId, P2LOutputStream outputStream) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.registerCustomOutputStream(to, toShort(messageType), toShort(conversationId), outputStream);
    }
    @Override public P2LFragmentInputStream createFragmentInputStream(SocketAddress from, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createFragmentInputStream(this, from, establishedConnections.get(from), toShort(messageType), toShort(conversationId));
    }
    @Override public P2LFragmentOutputStream createFragmentOutputStream(SocketAddress to, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createFragmentOutputStream(this, to, establishedConnections.get(to), toShort(messageType), toShort(conversationId));
    }
    @Override public void unregister(P2LInputStream stream) {
        incomingHandler.streamMessageHandler.unregister(stream);
    }
    @Override public void unregister(P2LOutputStream stream) {
        incomingHandler.streamMessageHandler.unregister(stream);
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
    @Override public P2LConnection getConnection(InetSocketAddress address) {
        return establishedConnections.get(address);
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
    @Override public void graduateToEstablishedConnection(P2LConnection peer, int conversationId) {
        if(peer == null)return; //used if client is already connected
        //PROBLEM: if the initiator of a connection tells the other side its public ip(which differs from package.getAddress() - because we are in the same, non public NAT)
        //   if they lie about who they are, then we will attempt a connection to the public ip - and DDOS it inadvertently
        boolean previouslyConnected = establishedConnections.put(peer.link.getSocketAddress(), peer) != null;
        historicConnections.remove(peer.link.getSocketAddress());
        if(!previouslyConnected)
            notifyConnectionEstablished(peer.link, conversationId);
    }
    @Override public void markBrokenConnection(P2Link address, boolean retry) {
        P2LConnection wasRemoved = address==null?null:establishedConnections.remove(address.getSocketAddress());
        if(wasRemoved==null) {
            System.err.println(address+" is not an established connection - could not mark as broken (already marked??)");
        } else {
            historicConnections.put(address.getSocketAddress(), new HistoricConnection(address, retry, wasRemoved.remoteBufferSize, wasRemoved.avRTT));
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
                graduateToEstablishedConnection(new P2LConnection(reEstablished.link, reEstablished.remoteBufferSize, reEstablished.avRTT), -1);//cool: auto remembering of correct(hopefully), link and buffer size...
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
    @Override public short createUniqueConversationId() {
        //Lock free - i hope
        while(true) {
            int id = runningConversationId.get();
            if(id+1 == NO_CONVERSATION_ID) {
                if(runningConversationId.compareAndSet(id, 1))
                    return 1;
            } else if(id+1 > Short.MAX_VALUE) {
                if(runningConversationId.compareAndSet(id, Short.MIN_VALUE))
                    return Short.MIN_VALUE;
            } else {
                if(runningConversationId.compareAndSet(id, id+1))
                    return (short) (id+1);
            }
        }
    }


    @Override public void printDebugInformation() {
        System.out.println("----- DEBUG INFORMATION -----");
        System.out.println("isClosed = "+incomingHandler.isClosed());
        System.out.println("connectionLimit = " + connectionLimit);
        System.out.println("establishedConnections("+establishedConnections.size()+") = " + establishedConnections);
        System.out.println("historicConnections("+historicConnections.size()+") = " + historicConnections);
        System.out.println("incomingHandler.broadcastState = " + incomingHandler.broadcastState.debugString());
        System.out.println("incomingHandler.messageQueue.debugString() = " + incomingHandler.messageQueue.debugString());
        System.out.println("incomingHandler.userBrdMessageQueue.debugString() = " + incomingHandler.brdMessageQueue.debugString());
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