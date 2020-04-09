package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.*;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.conversation.ConversationAnswererChangeThisName;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.protocols.*;
import jokrey.utilities.network.link2peer.node.stream.*;
import jokrey.utilities.network.link2peer.util.NetUtil;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.simple.data_structure.BadConcurrentMultiKeyMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.validateMsgTypeNotInternal;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.*;

/**
 *
 * NOT THREAD SAFE
 *
 * @author jokrey
 */
final class P2LNodeImpl implements P2LNode, P2LNodeInternal {
    private final IncomingHandler incomingHandler;
    private final P2LThreadPool outgoingPool = new P2LThreadPool(4, 64);

    /**
     * If the self link is not known, it has to be EXPLICITLY queried and SET as public link using the who am I protocol
     *  (because it is difficult to distinguish a local network ip from a public one
     *  Otherwise establishing connections will only be possible as a hidden node that requires relaying by the public node (a node will favor public links over hidden links in querying connections)
     */
    private P2Link selfLink;
    P2LNodeImpl(P2Link selfLink) throws IOException {
        this(selfLink, Integer.MAX_VALUE);
    }
    P2LNodeImpl(P2Link selfLink, int connectionLimit) throws IOException {
        setSelfLink(selfLink);
        this.connectionLimit = connectionLimit;

        incomingHandler = new IncomingHandler(this);

        new Thread(() -> {
            while(!incomingHandler.isClosed()) {
                long now = System.currentTimeMillis();
                try {
                    //this in the future required to keep nat holes alive - therefore it may need to be called more than the current every two minutes
                    //   however the default timeout appears to be 120-300 seconds, i.e. two to five minutes so 90 seconds should be fine

                    List<InetSocketAddress> dormant = getDormantEstablishedConnections(now);
                    P2LFuture<Boolean>[] pingResults = new P2LFuture[dormant.size()];
                    for (int i = 0; i < dormant.size(); i++)
                        pingResults[i] = PingProtocol.asInitiator(this, dormant.get(i));
                    List<InetSocketAddress> retryableHistoricConnections = getRetryableHistoricSocketAdresses(now);
                    for(InetSocketAddress retryable:retryableHistoricConnections)
                        outgoingPool.execute(() -> EstablishConnectionProtocol.asInitiator(this, null, retryable)); //result does not matter - initiator will internally graduate a successful connection - and the timeout is much less than 10000

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
        if(selfLink.isRelayed()) throw new IllegalArgumentException("self link cannot be relayed - use local link");
        this.selfLink = selfLink;
    }

    @Override public P2LFuture<P2Link.Direct> whoAmI(InetSocketAddress requestFrom) {
        return outgoingPool.execute(() -> WhoAmIProtocol.asInitiator(P2LNodeImpl.this, requestFrom));
    }

    @Override public void close() {
        disconnectFromAll();
        incomingHandler.close();
        outgoingPool.shutdown();
    }

    @Override public P2LFuture<Boolean> establishConnection(P2Link to) {
        return EstablishConnectionProtocol.asInitiator(this, to, null);
    }
    @Override public P2LFuture<Collection<P2Link>> establishConnections(P2Link... links) {
        LinkedList<P2LFuture<P2Link>> connectionResults = new LinkedList<>();
        for(P2Link link : links)
            connectionResults.addLast(EstablishConnectionProtocol.asInitiator(this, link, null).toType(success -> success?link:null));
        return P2LFuture.oneForAll(connectionResults);
    }
    @Override public void disconnectFrom(P2LConnection from) {
        DisconnectSingleConnectionProtocol.asInitiator(this, from.address);
    }

    @Override public List<P2Link> recursiveGarnerConnections(int newConnectionLimit, int newConnectionLimitPerRecursion, P2Link... setupLinks) {
        return GarnerConnectionsRecursivelyProtocol.recursiveGarnerConnections(this, newConnectionLimit, newConnectionLimitPerRecursion, Arrays.asList(setupLinks));
    }

    @Override public List<P2Link> queryKnownLinksOf(InetSocketAddress from) throws IOException {
        return RequestPeerLinksProtocol.asInitiator(this, from);
    }

    @Override public P2LFuture<Integer> sendBroadcastWithReceipts(P2LMessage message) {
        validateMsgTypeNotInternal(message.header.getType());

        incomingHandler.broadcastState.markAsKnown(message.getContentHash());

        return BroadcastMessageProtocol.relayBroadcast(this, message);
    }


    @Override public void registerInternalConversationFor(int type, ConversationAnswererChangeThisName handler) {
        incomingHandler.conversationMessageHandler.registerConversationHandlerFor(type, handler);
    }

    @Override public P2LConversation internalConvo(int type, int conversationId, InetSocketAddress to) {
        return incomingHandler.conversationMessageHandler.getOutgoingConvoFor(this, establishedConnections.getBy1(to), to, toShort(type), toShort(conversationId));
    }

    //DIRECT MESSAGING:
    @Override public void sendInternalMessage(InetSocketAddress to, P2LMessage message) throws IOException {
        if(message.canBeSentInSinglePacket()) {
            if(DebugStats.MSG_PRINTS_ACTIVE)
                System.out.println(getSelfLink()+" - sendInternalMessage - to = [" + to + "], message = " + message);
            incomingHandler.serverSocket.send(message.toPacket(to));
        } else {
            //todo - is it really desirable to have packages be broken up THIS automatically???
            //    - like it is cool that breaking up packages does not make a difference, but... like it is so transparent it could lead to inefficiencies
//            throw new IllegalStateException(message.size+"");
            System.out.println("sendLong(being broken up) - message = " + message + ", to = " + to);
            incomingHandler.longMessageHandler.send(this, message, to);
        }
    }
    @Override public P2LFuture<Boolean> sendInternalMessageWithReceipt(InetSocketAddress to, P2LMessage message) throws IOException {
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

    @Override public boolean sendInternalMessageWithRetries(InetSocketAddress to, P2LMessage message, int attempts) {
        P2LConnection con = establishedConnections.getBy1(to);
        if(con == null) throw new IllegalArgumentException("to("+to+") has to be an established connection - to automatically determine the required values");
        return sendInternalMessageWithRetries(message, to, attempts, (int) (con.avRTT*1.1));
    }

    @Override public P2LFuture<ReceivedP2LMessage> expectMessage(int messageType) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.messageQueue.futureFor(toShort(messageType));
    }
    @Override public P2LFuture<ReceivedP2LMessage> expectInternalMessage(InetSocketAddress from, int messageType) {
        return incomingHandler.messageQueue.futureFor(from, toShort(messageType));
    }
    @Override public P2LFuture<ReceivedP2LMessage> expectInternalMessage(InetSocketAddress from, int messageType, int conversationId) {
        return incomingHandler.messageQueue.futureFor(from, toShort(messageType), toShort(conversationId));
    }
    @Override public P2LFuture<P2LBroadcastMessage> expectBroadcastMessage(int messageType) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.brdMessageQueue.futureFor(toShort(messageType));
    }
    @Override public P2LFuture<P2LBroadcastMessage> expectBroadcastMessage(P2Link source, int messageType) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.brdMessageQueue.futureFor(source, toShort(messageType));
    }
    @Override public P2LOrderedInputStream createInputStream(InetSocketAddress from, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createInputStream(this, from, establishedConnections.getBy1(from), toShort(messageType), toShort(conversationId), NO_STEP);
    }
    @Override public boolean registerCustomInputStream(InetSocketAddress from, int messageType, int conversationId, P2LInputStream inputStream) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createCustomInputStream(from, toShort(messageType), toShort(conversationId), NO_STEP, inputStream);
    }
    @Override public P2LOrderedOutputStream createOutputStream(InetSocketAddress to, int messageType, int conversationId) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.createOutputStream(this, to, establishedConnections.getBy1(to), toShort(messageType), toShort(conversationId), NO_STEP);
    }
    @Override public boolean registerCustomOutputStream(InetSocketAddress to, int messageType, int conversationId, P2LOutputStream outputStream) {
        validateMsgTypeNotInternal(messageType);
        return incomingHandler.streamMessageHandler.registerCustomOutputStream(to, toShort(messageType), toShort(conversationId), NO_STEP, outputStream);
    }
    @Override public P2LFragmentInputStream createFragmentInputStream(InetSocketAddress from, short type, short conversationId, short step) {
        return incomingHandler.streamMessageHandler.createFragmentInputStream(this, from, establishedConnections.getBy1(from), type, conversationId, step);
    }
    @Override public P2LFragmentOutputStream createFragmentOutputStream(InetSocketAddress from, short type, short conversationId, short step) {
        return incomingHandler.streamMessageHandler.createFragmentOutputStream(this, from, establishedConnections.getBy1(from), type, conversationId, step);
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
    private final BadConcurrentMultiKeyMap<InetSocketAddress, P2Link, P2LConnection> establishedConnections = new BadConcurrentMultiKeyMap<>();

    /**
     * previously established connections
     * the value(the long) here indicates the time in ms since 1970, at which point a retry connection attempt should be made to the connection
     */
    private final BadConcurrentMultiKeyMap<InetSocketAddress, P2Link, HistoricConnection> historicConnections = new BadConcurrentMultiKeyMap<>();
    private final int connectionLimit;
    @Override public boolean isConnectedTo(P2Link to) {
        return establishedConnections.containsBy2(to);
    }
    @Override public boolean isConnectedTo(InetSocketAddress to) {
        return establishedConnections.containsBy1(to);
    }
    @Override public InetSocketAddress resolve(P2Link link) {
        if (link.isDirect()) {
            return ((P2Link.Direct) link).resolve();
        } else if(link.isOnlyLocal()) {
            return ((P2Link.Local) link).unsafeAsDirect().resolve();
        } else {
            System.out.println("link = " + link);
            P2LConnection con = establishedConnections.getBy2(link);
            return con == null? null: con.address;
        }
    }

    @Override public InetSocketAddress resolveByName(String link) {
        P2LConnection con = establishedConnections.getBy2(new P2Link.Local(link, -1));
        return con==null?null:con.address; //this works, because local and relayed can equal and are only identified by their name field....
    }
    @Override public P2LConnection getConnection(P2Link link) {
        return establishedConnections.getBy2(link);
    }
    @Override public P2LConnection getConnection(InetSocketAddress address) {
        return establishedConnections.getBy1(address);
    }

    @Override public P2LConnection[] getEstablishedConnections() {
        return establishedConnections.values(new P2LConnection[0]);
    }
    @Override public HistoricConnection[] getPreviouslyEstablishedConnections() {
        return historicConnections.values(new HistoricConnection[0]);
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
        boolean previouslyConnected = establishedConnections.put(peer.address, peer.link, peer) != null;
        historicConnections.removeBy2(peer.link);
        if(!previouslyConnected)
            notifyConnectionEstablished(peer, conversationId);
    }
    @Override public void markBrokenConnection(InetSocketAddress address, boolean retry) {
        P2LConnection wasRemoved = address==null?null:establishedConnections.removeBy1(address);
        if(wasRemoved==null) {
            System.err.println(address+" is not an established connection - could not mark as broken (already marked??)");
        } else {
            historicConnections.put(address, wasRemoved.link, new HistoricConnection(wasRemoved.link, address, retry, wasRemoved.remoteBufferSize, wasRemoved.avRTT));
            notifyConnectionDisconnected(wasRemoved);
        }
    }
    @Override public void notifyPacketReceivedFrom(InetSocketAddress from) {
        //todo this operation may be to slow to compute EVERY TIME a packet is received - on the other hand it is very convenient..
        P2LConnection established = establishedConnections.getBy1(from);
        if(established!=null) {
            established.notifyActivity();
        } else {
            HistoricConnection reEstablished = historicConnections.removeBy1(from);
            if(reEstablished != null/* && retryStateOfHistoricConnection.r>0*/) //actively retrying IS NOT REQUIRED, the connection should be reestablished nonetheless
                graduateToEstablishedConnection(new P2LConnection(reEstablished.link, reEstablished.address, reEstablished.remoteBufferSize, reEstablished.avRTT), -1);//cool: auto remembering of correct(hopefully), link and buffer size...
        }
    }
    private List<InetSocketAddress> getDormantEstablishedConnections(long now) {
        ArrayList<InetSocketAddress> dormantConnections = new ArrayList<>(establishedConnections.size());
        for(P2LConnection e:establishedConnections.values(new P2LConnection[0]))
            if(e.isDormant(now))
                dormantConnections.add(e.address);
        return dormantConnections;
    }
    /** Already sets the new retry time - so after using this method it is mandatory to actually retry the given connections  */
    private List<InetSocketAddress> getRetryableHistoricSocketAdresses(long now) {
        ArrayList<InetSocketAddress> retryableHistoricConnections = new ArrayList<>(Math.min(16, historicConnections.size()));
        for(HistoricConnection e:historicConnections.values(new HistoricConnection[0]))
            if(e.shouldRetryNow(now))
                retryableHistoricConnections.add(e.address);
        return retryableHistoricConnections;
    }

    @Override public P2LFuture<Integer> executeAllOnSendThreadPool(P2LThreadPool.Task... tasks) {
        return outgoingPool.execute(tasks);
    }

    private final InterfaceAddress ip4InterfaceAddress = NetUtil.getLocalIPv4InterfaceAddress(); //might take a while to instantiate - so we have to do it before
    @Override public InterfaceAddress getLocalIPv4InterfaceAddress() {
        return ip4InterfaceAddress;
    }
    private InterfaceAddress ip6InterfaceAddress = NetUtil.getLocalIPv4InterfaceAddress(); //might take a while to instantiate - so we have to do it before
    @Override public InterfaceAddress getLocalIPv6InterfaceAddress() {
        return ip6InterfaceAddress;
    }

    //LISTENERS:
    private final ArrayList<P2LMessageListener<ReceivedP2LMessage>> individualMessageListeners = new ArrayList<>();
    private final ArrayList<P2LMessageListener<P2LBroadcastMessage>> broadcastMessageListeners = new ArrayList<>();
    private final ArrayList<BiConsumer<P2LConnection, Integer>> newConnectionEstablishedListeners = new ArrayList<>();
    private final ArrayList<Consumer<P2LConnection>> connectionDisconnectedListeners = new ArrayList<>();
    @Override public void addMessageListener(P2LMessageListener<ReceivedP2LMessage> listener) { individualMessageListeners.add(listener); }
    @Override public void addBroadcastListener(P2LMessageListener<P2LBroadcastMessage> listener) { broadcastMessageListeners.add(listener); }
    @Override public void addConnectionEstablishedListener(BiConsumer<P2LConnection, Integer> listener) { newConnectionEstablishedListeners.add(listener); }
    @Override public void addConnectionDroppedListener(Consumer<P2LConnection> listener) { connectionDisconnectedListeners.add(listener); }
    @Override public void removeMessageListener(P2LMessageListener<ReceivedP2LMessage> listener) { individualMessageListeners.remove(listener); }
    @Override public void removeBroadcastListener(P2LMessageListener<P2LBroadcastMessage> listener) { broadcastMessageListeners.remove(listener); }
    @Override public void removeConnectionEstablishedListener(BiConsumer<P2LConnection, Integer> listener) { newConnectionEstablishedListeners.remove(listener); }
    @Override public void removeConnectionDroppedListener(Consumer<P2LConnection> listener) { connectionDisconnectedListeners.remove(listener); }

    @Override public void notifyUserBroadcastMessageReceived(P2LBroadcastMessage message) {
        for (P2LMessageListener<P2LBroadcastMessage> l : broadcastMessageListeners) { l.received(message); }
    }
    @Override public void notifyUserMessageReceived(ReceivedP2LMessage message) {
        for (P2LMessageListener<ReceivedP2LMessage> l : individualMessageListeners) { l.received(message); }
    }

    private void notifyConnectionEstablished(P2LConnection newAddress, int conversationId) {
        for (BiConsumer<P2LConnection, Integer> l : (List<BiConsumer<P2LConnection, Integer>>) newConnectionEstablishedListeners.clone()) { l.accept(newAddress, conversationId); }
    }
    private void notifyConnectionDisconnected(P2LConnection newAddress) {
        for (Consumer<P2LConnection> l : connectionDisconnectedListeners) { l.accept(newAddress); }
    }


    private AtomicInteger runningConversationId = new AtomicInteger(NO_CONVERSATION_ID + 1);
//    private AtomicInteger runningConversationId = new AtomicInteger(ThreadLocalRandom.current().nextInt(Short.MAX_VALUE/2));
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