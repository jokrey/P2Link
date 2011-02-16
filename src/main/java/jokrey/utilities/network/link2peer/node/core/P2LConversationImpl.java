package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.message_headers.ConversationHeader;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * todo For few dropped packages the number of double resend packages is relatively high
 * todo Problem: Consider packets m0 and m1 and m2. client sends m0, server received m0 and sends m1, m1 is lost.
 * todo          Since m1 is the ack for m0 -> client resends m0, roughly exactly when m0 arrieves the server realizes that it has not received m2 and resends m1.
 * todo          Despite only m1 being lost m0 is resent
 * todo   This problem may very well be unsolvable, but it should be kept in mind
 * todo     - OPTION: it could be possible to have the server never resend, UNLESS the client resends...
 *
 *
 * todo - these methods use blocking synchronization... It should probably be unblocking...
 */
public class P2LConversationImpl implements P2LConversation {
    public static int DEFAULT_NUM_ATTEMPTS = 4;
    public static float DEFAULT_M = 1.25f;
    public static int DEFAULT_A = 25;

    public final P2LNodeInternal parent;
    private final P2LMessageQueue conversationQueue;
    private final P2LConnection con;
    public final SocketAddress peer;
    private final short type, conversationId;

    private short step = -2;

    private int maxAttempts = DEFAULT_NUM_ATTEMPTS; @Override public void setMaxAttempts(int maxAttempts) {this.maxAttempts = maxAttempts;}
    private float m = DEFAULT_M; @Override public void setM(float m) {this.m = m;}
    private int a = DEFAULT_A; @Override public void setA(int a) {this.a = a;}
    public P2LConversationImpl(P2LNodeInternal parent, P2LMessageQueue conversationQueue, P2LConnection con, SocketAddress peer, short conversationId, short type) {
        this.parent = parent;
        this.conversationQueue = conversationQueue;
        this.type = type;
        this.conversationId = conversationId;
        this.peer = peer;
        if(con == null)
            this.con = new P2LConnection(null, 1024, -1);
        else
            this.con = con;
    }

    @Override public SocketAddress getPeer() { return peer; }
    @Override public int getAvRTT() { return con.avRTT; }

    private int calcWaitBeforeRetryTime() {
        return con.avRTT == -1? 500 :(int) (con.avRTT * m + a);
    }

    private boolean isServer = false;
    void serverInit(P2LMessage m0) {
        step=0;
        isServer = true;
        lastMessageReceived = m0;
    }

    private long lastMessageSentAt = 0;
    private P2LMessage lastMessageReceived = null;
    @Override public P2LMessage initExpectMsg(byte[] bytes) throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.initExpect: "+"bytes = [" + Arrays.toString(bytes) + "]");
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        return answerExpectMsg(bytes, true);
    }
    @Override public P2LMessage initExpectCloseMsg(byte[] bytes) throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.initExpect: "+"bytes = [" + Arrays.toString(bytes) + "]");
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        P2LMessage result = answerExpectMsg(bytes, true);
        step = -1;
        return result;
    }
    @Override public P2LMessage answerExpectMsg(byte[] bytes) throws IOException {
        return answerExpectMsg(bytes, false);
    }


    @Override public void close() throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        parent.sendInternalMessage(peer, lastMessageReceived.createReceipt());
        step = -1;
    }
    @Override public void closeWith(byte[] bytes) throws IOException {
        if(!isServer) throw new IllegalStateException("can only be used when server");
        if(step != 0) throw new IllegalStateException("can only be used as first instruction");
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);
        step = -1;
        parent.sendInternalMessage(peer, msg);
    }

    @Override public void initClose(byte[] bytes) throws IOException {
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        answerClose(bytes, true);
        step = -1;
    }

    @Override public void answerClose(byte[] bytes) throws IOException {
        answerClose(bytes, false);
    }



    private P2LMessage answerExpectMsg(byte[] bytes, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");

        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);

        for(int i=0;i<maxAttempts;i++) {
            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
            P2LFuture<P2LMessage> previous = init?null:conversationQueue.futureFor(peer, type, conversationId, step);
            P2LFuture<P2LMessage> next = conversationQueue.futureFor(peer, type, conversationId, (short) (step + 1));


            parent.sendInternalMessage(peer, msg);
            lastMessageSentAt = System.currentTimeMillis();

            P2LMessage result = next.getOrNull(calcWaitBeforeRetryTime());
            if(previous != null) {
                boolean wasCompleted = previous.cancelIfNotCompleted();
                if(wasCompleted)
                    DebugStats.conversation_numDoubleReceived.getAndIncrement();
            }
            if (result != null) {
                lastMessageReceived = result;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();
                return result;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }
    private void answerClose(byte[] bytes, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);

        for(int i=0;i<maxAttempts;i++) {
            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
            P2LFuture<P2LMessage> previous = init?null:conversationQueue.futureFor(peer, type, conversationId, step);
            P2LFuture<P2LMessage> fut = conversationQueue.receiptFutureFor(peer, type, conversationId, isServer ? (short) (step + 1) : step);

            lastMessageSentAt = System.currentTimeMillis();
            parent.sendInternalMessage(peer, msg);

            P2LMessage result = fut.getOrNull(calcWaitBeforeRetryTime());
            if(previous != null) {
                boolean wasCompleted = previous.cancelIfNotCompleted();
                if(wasCompleted)
                    DebugStats.conversation_numDoubleReceived.getAndIncrement();
            }
            if (result != null && result.validateIsReceiptFor(msg)) {
                lastMessageReceived = result;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step = -1;
                DebugStats.conversation_numValid.getAndIncrement();
                return;
            } else {
                msg.mutateToRequestReceipt();
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }
}