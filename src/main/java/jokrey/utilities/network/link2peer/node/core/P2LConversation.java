package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.message_headers.ConversationHeader;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;

/**
 * Forced efficiency, short burst communication that uses the response as validation for receival.
 *
 * @author jokrey
 */
public class P2LConversation {
    public static int DEFAULT_NUM_RETRIES = 100;
    public static float DEFAULT_M = 1.25f;
    public static int DEFAULT_A = 25;

    private final P2LNodeInternal parent;
    private final P2LMessageQueue conversationQueue;
    private final P2LConnection con;
    private final short type, conversationId;

    private short step = -2;

    private int maxRetries = DEFAULT_NUM_RETRIES; public void setMaxRetries(int maxRetries) {this.maxRetries = maxRetries;}
    private float m = DEFAULT_M; public void setM(float m) {this.m = m;}
    private int a = DEFAULT_A; public void setA(int a) {this.a = a;}
    public P2LConversation(P2LNodeInternal parent, P2LMessageQueue conversationQueue, P2LConnection con, short type, short conversationId) {
        this.parent = parent;
        this.conversationQueue = conversationQueue;
        this.con = con;
        this.type = type;
        this.conversationId = conversationId;
    }

    private long lastMessageSentAt = 0;
    private P2LMessage lastMessageReceived = null;
    public byte[] initExpect(byte[] bytes) throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.initExpect: "+"bytes = [" + Arrays.toString(bytes) + "]");
        if(step != -2) throw new IOException("cannot init twice");

        step = 0;

        return answerExpect(bytes);
    }
    public byte[] answerExpect(byte[] bytes) throws IOException {
        if(step == -1) throw new IOException("conversation is closed");
        if(step == -2) throw new IOException("please initialize");
//        System.out.println(parent.getSelfLink()+" - P2LConversation.answerExpect: "+"bytes = [" + Arrays.toString(bytes) + "]");

        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);

        for(int i=0;i<maxRetries;i++) {
            P2LFuture<P2LMessage> fut = conversationQueue.futureFor(con.link.getSocketAddress(), type, conversationId, (short) (step + 1));

            parent.sendInternalMessage(con.link.getSocketAddress(), msg);
            lastMessageSentAt = System.currentTimeMillis();

            int waitTime = (int) (con.avRTT * m + a);
//            System.out.println(parent.getSelfLink() + " - expect: " + type + ", " + conversationId + ", " + (step + 1));
            P2LMessage result = fut.getOrNull(waitTime);
//            System.out.println(parent.getSelfLink() + " - result = " + result);
            if (result != null) {
                lastMessageReceived = result;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                return result.asBytes();
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }


    public void close() throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.close");
        step = -1;
        parent.sendInternalMessage(con.link.getSocketAddress(), lastMessageReceived.createReceipt());
    }
    public void answerClose(byte[] bytes) throws IOException {
//        System.out.println(parent.getSelfLink()+" - P2LConversation.answerClose: "+"bytes = [" + Arrays.toString(bytes) + "]");
        ConversationHeader header = new ConversationHeader(null, type, conversationId, isServer ? (short) (step + 1) : step, false);
        P2LMessage msg = header.generateMessage(bytes);

        for(int i=0;i<maxRetries;i++) {
            P2LFuture<P2LMessage> fut = conversationQueue.receiptFutureFor(con.link.getSocketAddress(), type, conversationId, (step));

            lastMessageSentAt = System.currentTimeMillis();
            parent.sendInternalMessage(con.link.getSocketAddress(), msg);

            int waitTime = (int) (con.avRTT * m + a);
//            System.out.println(parent.getSelfLink() + " - expect: " + type + ", " + conversationId + ", " + (step  ));
            P2LMessage result = fut.getOrNull(waitTime);
//            System.out.println(parent.getSelfLink() + " - result = " + result);
            if (result != null && result.validateIsReceiptFor(msg)) {
                lastMessageReceived = result;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step = -1;
                return;
            } else {
                msg.mutateToRequestReceipt();
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }

    private boolean isServer = false;
    public void serverInit() {
        step=0;
        isServer = true;
    }
}