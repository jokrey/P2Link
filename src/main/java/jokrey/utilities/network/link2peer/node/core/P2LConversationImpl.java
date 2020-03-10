package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
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
    public static int DEFAULT_rM = 100;

    public final P2LNodeInternal parent;
    private final P2LMessageQueue conversationQueue;
    private final P2LConnection con;
    public final SocketAddress peer;
    private final short type, conversationId;
    private final int headerSize;

    private short step = -2;

    private int maxAttempts = DEFAULT_NUM_ATTEMPTS; @Override public void setMaxAttempts(int maxAttempts) {
        if(maxAttempts < 1) throw new IllegalArgumentException("cannot not attempt to send packages - i mean we can, but we don't need an api for that");
        this.maxAttempts = maxAttempts;
    }
    private float m = DEFAULT_M; @Override public void setM(float m) {
        if(m <= 1f) throw new IllegalArgumentException("impossible to use m smaller or equal to 1, packages would be resent before they could possibly be answered(on average)");
        this.m = m;
    }
    private int a = DEFAULT_A; @Override public void setA(int a) {
        if(a <= 0) throw new IllegalArgumentException("cannot have negative or equal to 0 a. would mean that there would be NO conceptual time to compute an answer");
        this.a = a;
    }
    private int rM = DEFAULT_rM; @Override public void setRM(int rM) {
        if(rM < 0) throw new IllegalArgumentException("cannot be negative");
        this.rM = rM;
    }
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

        headerSize = new ConversationHeader(null, type, conversationId, step, false).getSize();
    }

    @Override public SocketAddress getPeer() { return peer; }
    @Override public int getAvRTT() { return con.avRTT; }
    @Override public int getHeaderSize() { return headerSize; }
    @Override public int getMaxPayloadSizePerPackage() {
        return con.remoteBufferSize - headerSize;
    }

    private int calcWaitBeforeRetryTime(int numPreviousRetries) {
        return ( con.avRTT == -1? 500 :(int) (con.avRTT * m + a) )
                + rM*numPreviousRetries;//todo - do this? helped to reduce number of timeouts in catch up tests
    }

    private boolean isServer = false;
    void serverInit(P2LMessage m0) {
        step=0;
        isServer = true;
        lastMessageReceived = m0;
    }

    private long lastMessageSentAt = 0;
    private P2LMessage lastMessageReceived = null;
    //ONLY ON CLIENT as first convo call, requires an answerExpect on the other side after
    @Override public P2LMessage initExpect(MessageEncoder encoded) throws IOException {
        if(isServer) throw new IOException("not client (server init is automatic, use 'answerExpect')");
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        return answerExpect(encoded, true);
    }
    //requires and answerExpect or initExpect on the other side before, and an answerExpect or answerClose after
    @Override public P2LMessage answerExpect(MessageEncoder encoded) throws IOException {
        return answerExpect(encoded, false);
    }

    private P2LMessage answerExpect(MessageEncoder encoded, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");

        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = P2LMessage.from(header, encoded);

//        System.out.println("P2LConversationImpl.answerExpectMsg - "+"init = " + init+", step = " + step+", isServer = " + isServer);

        for(int i=0;i<maxAttempts;i++) {
            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
            P2LFuture<P2LMessage> previous = init?null:conversationQueue.futureFor(peer, type, conversationId, step);
            P2LFuture<P2LMessage> next = conversationQueue.futureFor(peer, type, conversationId, (short) (step + 1));


            parent.sendInternalMessage(peer, msg);
            lastMessageSentAt = System.currentTimeMillis();

            P2LMessage result = next.getOrNull(calcWaitBeforeRetryTime(i));
            if(previous != null) {
                boolean wasCompleted = ! previous.cancelIfNotCompleted();
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





    //REGULAR CLOSE

    //requires a close on the other side
    @Override public void answerClose(MessageEncoder encoded) throws IOException {
        answerClose(encoded, false);
    }

    //requires an answerClose on the other side
    @Override public void close() throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        //if this message is lost answerClose will resent, but the message will not be handled by a handler - requestReceipt hits and a receipt equal to this will be sent
        parent.sendInternalMessage(peer, lastMessageReceived.createReceipt());
        step = -1;
    }

    private void answerClose(MessageEncoder encoded, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        //always requests receipt, but only receives it through that if the remote was not waiting for it. Otherwise answerExpect returns and sends close
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, true);
        P2LMessage msg = P2LMessage.from(header, encoded);

        for(int i=0;i<maxAttempts;i++) {
            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
            P2LFuture<P2LMessage> previous = init?null:conversationQueue.futureFor(peer, type, conversationId, step);
            P2LFuture<P2LMessage> last = conversationQueue.receiptFutureFor(peer, type, conversationId, isServer ? (short) (step + 1) : step);

            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
            parent.sendInternalMessage(peer, msg);

            P2LMessage result = last.getOrNull(calcWaitBeforeRetryTime(i));
            if(previous != null) {
                boolean wasCompleted = ! previous.cancelIfNotCompleted();
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
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }






    //DIRECT CLOSE
    //As first and last on client side - needs a closeWith(data) on server side
    //NOTE:: Very possible that server code is executed multiple times -
    @Override public P2LMessage initExpectClose(MessageEncoder encoded) throws IOException {
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        P2LMessage result = answerExpect(encoded, true);
        step = -1;
        return result;
    }
    //As first and last on server side - needs a initExpectClose(data) on client side  - todo convert to answer close, use internally...
    @Override public void closeWith(MessageEncoder encoded) throws IOException {
        if(!isServer) throw new IllegalStateException("can only be used when server");
        if(step != 0) throw new IllegalStateException("can only be used as first instruction");
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = P2LMessage.from(header, encoded);
        step = -1;
        parent.sendInternalMessage(peer, msg);
    }






    //PAUSE

    @Override public P2LMessage initExpectAfterPause(MessageEncoder encoded, int timeout) throws IOException {
        if(isServer) throw new IOException("not client (server init is automatic, use 'answerExpect')");
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        return answerExpectAfterPause(encoded, timeout, true);
    }
    @Override public P2LMessage answerExpectAfterPause(MessageEncoder encoded, int timeout) throws IOException {
        return answerExpectAfterPause(encoded, timeout, false);
    }

    private P2LMessage answerExpectAfterPause(MessageEncoder encoded, int timeout, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        //does not request a receipt, but expects to receive a receipt. - resends shall not trigger automatic receipt resends(compare to close, where that is desired)
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = P2LMessage.from(header, encoded);

        for(int i=0;i<maxAttempts;i++) {
            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
            P2LFuture<P2LMessage> previous = init?null:conversationQueue.futureFor(peer, type, conversationId, step);
            P2LFuture<P2LMessage> ack = conversationQueue.receiptFutureFor(peer, type, conversationId, isServer ? (short) (step + 1) : step);

            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
            parent.sendInternalMessage(peer, msg);

            P2LMessage result = ack.getOrNull(calcWaitBeforeRetryTime(i));
            if(previous != null) {
                boolean wasCompleted = ! previous.cancelIfNotCompleted();
                if(wasCompleted)
                    DebugStats.conversation_numDoubleReceived.getAndIncrement();
            }
            if (result != null && result.validateIsReceiptFor(msg)) {
                lastMessageReceived = result;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();

                P2LFuture<P2LMessage> next = conversationQueue.futureFor(peer, type, conversationId, step);

                result = next.get(timeout); //will throw the same timeout exception timeout is reached, -1 means timeout is never reached
                lastMessageReceived = result;
                //cannot update avRTT, since we have no idea when this result message was sent on the other side(somewhere between lastMessageSent and now)
                DebugStats.conversation_numValid.getAndIncrement();

                return result;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }

    @Override public void pause() throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        parent.sendInternalMessage(peer, lastMessageReceived.createReceipt());
    }




    @Override public void initClose(MessageEncoder encoded) throws IOException {
        if(isServer) throw new IOException("init only possible on client side, server does this automatically, use closeWith or answerClose");
        if(step != -2) throw new IOException("cannot init twice");
        step = 0;
        answerClose(encoded, true);
        step = -1;
    }
}