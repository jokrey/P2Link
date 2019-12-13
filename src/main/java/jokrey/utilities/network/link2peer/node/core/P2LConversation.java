package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.message_headers.ConversationHeader;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * Forced efficiency, short burst communication that uses the response as validation for receival.
 *    Tcp - in the optimal case - should work similarly and have similar performance...
 *
 * todo For few dropped packages the number of double resend packages is relatively high
 * todo Problem: Consider packets m0 and m1 and m2. client sends m0, server received m0 and sends m1, m1 is lost.
 * todo          Since m1 is the ack for m0 -> client resends m0, roughly exactly when m0 arrieves the server realizes that it has not received m2 and resends m1.
 * todo          Despite only m1 being lost m0 is resent
 * todo   This problem may very well be unsolvable, but it should be kept in mind
 *
 * todo -
 *
 * @author jokrey
 */
public class P2LConversation {
    public static int DEFAULT_NUM_RETRIES = 100;
    public static float DEFAULT_M = 1.25f;
    public static int DEFAULT_A = 25;

    public final P2LNodeInternal parent;
    private final P2LMessageQueue conversationQueue;
    private final P2LConnection con;
    private final short type, conversationId;
    
    public SocketAddress getPeer() {
        return con.link.getSocketAddress();
    }

    private short step = -2;

    private int maxRetries = DEFAULT_NUM_RETRIES; public void setMaxRetries(int maxRetries) {this.maxRetries = maxRetries;}
    private float m = DEFAULT_M; public void setM(float m) {this.m = m;}
    private int a = DEFAULT_A; public void setA(int a) {this.a = a;}
    public P2LConversation(P2LNodeInternal parent, P2LMessageQueue conversationQueue, P2LConnection con, short type, short conversationId) {
        this.parent = parent;
        this.conversationQueue = conversationQueue;
        this.type = type;
        this.conversationId = conversationId;
        if(con == null)
            this.con = new P2LConnection(null, 1024, -1);
        else
            this.con = con;
    }

    private int calcWaitBeforeRetryTime() {
        return con.avRTT == -1? 500 :(int) (con.avRTT * m + a);
    }

    private boolean isServer = false;
    void serverInit() {
        step=0;
        isServer = true;
    }

    private long lastMessageSentAt = 0;
    private P2LMessage lastMessageReceived = null;
    public byte[] initExpect(byte[] bytes) throws IOException {
        return initExpectMsg(bytes).asBytes();
    }
    public P2LMessage initExpectMsg(byte[] bytes) throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.initExpect: "+"bytes = [" + Arrays.toString(bytes) + "]");
        if(step != -2) throw new IOException("cannot init twice");

        step = 0;

        return answerExpectMsg(bytes, true);
    }
    public byte[] answerExpect(byte[] bytes) throws IOException {
        return answerExpectMsg(bytes).asBytes();
    }
    public P2LMessage answerExpectMsg(byte[] bytes) throws IOException {
        return answerExpectMsg(bytes, false);
    }
    private P2LMessage answerExpectMsg(byte[] bytes, boolean init) throws IOException {
        if(step == -1) throw new IOException("conversation is closed");
        if(step == -2) throw new IOException("please initialize");
//        System.out.println(parent.getSelfLink()+" - P2LConversation.answerExpect: "+"bytes = [" + Arrays.toString(bytes) + "]");

        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);

        for(int i=0;i<maxRetries;i++) {
            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
            P2LFuture<P2LMessage> previous = init?null:conversationQueue.futureFor(getPeer(), type, conversationId, step);
            P2LFuture<P2LMessage> next = conversationQueue.futureFor(getPeer(), type, conversationId, (short) (step + 1));


            parent.sendInternalMessage(getPeer(), msg);
            lastMessageSentAt = System.currentTimeMillis();

//            System.out.println(parent.getSelfLink() + " - expect: " + type + ", " + conversationId + ", " + (step + 1));

            P2LMessage result = next.getOrNull(calcWaitBeforeRetryTime());
            if(previous != null) previous.cancelIfNotCompleted();
//            System.out.println(parent.getSelfLink() + " - result = " + result);
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


    public void close() throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.close");
        step = -1;
        parent.sendInternalMessage(getPeer(), lastMessageReceived.createReceipt());
    }
    public void answerClose(byte[] bytes) throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.answerClose: "+"bytes = [" + Arrays.toString(bytes) + "]");
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);

        for(int i=0;i<maxRetries;i++) {
            P2LFuture<P2LMessage> previous = conversationQueue.futureFor(getPeer(), type, conversationId, step); //to consume
            P2LFuture<P2LMessage> fut = conversationQueue.receiptFutureFor(getPeer(), type, conversationId, isServer ? (short) (step + 1) : step);

            lastMessageSentAt = System.currentTimeMillis();
            parent.sendInternalMessage(getPeer(), msg);

//            System.out.println(parent.getSelfLink() + " - expect: " + type + ", " + conversationId + ", " + (isServer ? (short) (step + 1) : step));
            P2LMessage result = fut.getOrNull(calcWaitBeforeRetryTime());
            if(previous != null) previous.cancelIfNotCompleted();
//            System.out.println(parent.getSelfLink() + " - result = " + result);
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