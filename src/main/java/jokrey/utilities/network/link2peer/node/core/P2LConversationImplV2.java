package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.message_headers.ConversationHeader;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * todo For few dropped packages the number of double resend packages is relatively high
 * todo Problem: Consider packets m0 and m1 and m2. client sends m0, server received m0 and sends m1, m1 is lost.
 * todo          Since m1 is the ack for m0 -> client resends m0, roughly exactly when m0 arrieves the server realizes that it has not received m2 and resends m1.
 * todo          Despite only m1 being lost m0 is resent
 * todo   This problem may very well be unsolvable, but it should be kept in mind
 * todo     - OPTION: it could be possible to have the server never resend, UNLESS the client resends...
 */
public class P2LConversationImplV2 implements P2LConversation {
    public static int DEFAULT_NUM_ATTEMPTS = 4;
    public static float DEFAULT_M = 1.25f;
    public static int DEFAULT_A = 25;
    public static int DEFAULT_rM = 100;

    public final P2LNodeInternal parent;
    public final ConversationHandlerV2 handler;
    private final P2LConnection con;
    public final SocketAddress peer;
    final short type;
    final short conversationId;
    private final int headerSize;

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
    public P2LConversationImplV2(P2LNodeInternal parent, ConversationHandlerV2 handler,
                                 P2LConnection con, SocketAddress peer, short type, short conversationId) {
        //SHALL NOT HAVE SIDE EFFECTS, BECAUSE THE CONVERSATION MIGHT BE DESTROYED INSTANTLY
        this.parent = parent;
        this.handler = handler;
        this.peer = peer;
        this.type = type;
        this.conversationId = conversationId;
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



    private short step = -2;
    private boolean pausedMode = false;
    private boolean pausedDoubleExpectMode = false; //todo - is there a clean way? this works, probably even best, but using an extra bool is a bit annoying to me

    private boolean isServer = false;
    public void notifyServerInit(ConversationAnswererChangeThisName handler, P2LMessage m0) throws IOException {
        if(step == -2) {
            step = 1;
            isServer = true;
            lastMessageReceived = m0;

            handler.converse(this, m0);
        } else {
            DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }

    private P2LMessage lastMessageReceived = null;
    public void notifyReceived(P2LMessage received) throws IOException {
        if(pausedMode) {
            parent.sendInternalMessage(peer, received.createReceipt());
        }
        if(received.header.getStep() == step || (pausedDoubleExpectMode && received.header.getStep() == step+1)) {
            System.out.println(parent.getSelfLink() + " - P2LConversationImplV2.notifyReceived - received = " + received);
            lastMessageReceived = received;
            SyncHelp.notify(this);
        } else {
            System.out.println(parent.getSelfLink() + " - P2LConversationImplV2.notifyReceived - received wrong(expected="+step+") = " + received);
            DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }



    private long lastMessageSentAt = 0;
    //ONLY ON CLIENT as first convo call, requires an answerExpect on the other side after
    @Override public P2LMessage initExpect(MessageEncoder encoded) throws IOException {
        if(isServer) throw new IllegalStateException("not client (server init is automatic, use 'answerExpect')");
        if(step != -2) throw new IllegalStateException("cannot init twice");
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
        pausedMode = false;

        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, encoded);

        step++;

        for(int i=0;i<maxAttempts;i++) {
            parent.sendInternalMessage(peer, msg);
            lastMessageSentAt = System.currentTimeMillis();

            boolean messageAvailable = SyncHelp.waitUntil(this, () -> lastMessageReceived != null && lastMessageReceived.header.getStep() == step, calcWaitBeforeRetryTime(i));
            System.out.println(parent.getSelfLink()+" - messageAvailable = " + messageAvailable);
            System.out.println(parent.getSelfLink()+" - lastMessageReceived = " + lastMessageReceived);
            System.out.println(parent.getSelfLink()+" - step = " + step);

            if (messageAvailable) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();
                return lastMessageReceived;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException();
    }





    //REGULAR CLOSE

    //NOTE: KNOWN UNSOLVABLE PROBLEM:
    //      If the receipt package for close is lost, the other side will retry requesting a new receipt.
    //      Both packages have to make it through, since only the answerClose side is retrying now...

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
        handler.remove(this);
    }

    private void answerClose(MessageEncoder encoded, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        pausedMode = false;
        //always requests receipt, but only receives it through that if the remote was not waiting for it. Otherwise answerExpect returns and sends close
        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, true);
        P2LMessage msg = P2LMessage.from(header, encoded);

        for(int i=0;i<maxAttempts;i++) {
            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
            parent.sendInternalMessage(peer, msg);

            boolean messageAvailable = SyncHelp.waitUntil(this, () -> lastMessageReceived != null && lastMessageReceived.header.getStep() == step, calcWaitBeforeRetryTime(i));

            if (messageAvailable && lastMessageReceived.validateIsReceiptFor(msg)) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step = -1;
                handler.remove(this);
                DebugStats.conversation_numValid.getAndIncrement();
                return;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        handler.remove(this);
        throw new TimeoutException();
    }






    //DIRECT CLOSE
    @Override public P2LMessage initExpectClose(MessageEncoder encoded) throws IOException {
        if(step != -2) throw new IllegalStateException("cannot init twice");
        step = 0;
        P2LMessage result = answerExpect(encoded, true);
        step = -1;
        handler.remove(this);
        return result;
    }
    @Override public void closeWith(MessageEncoder encoded) throws IOException {
        if(!isServer) throw new IllegalStateException("can only be used when server");
        if(step != 1) throw new IllegalStateException("can only be used as first instruction");
        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, encoded);
        step = -1;
        handler.remove(this);
        parent.sendInternalMessage(peer, msg);
    }
    @Override public void initClose(MessageEncoder encoded) throws IOException {
        if(isServer) throw new IllegalStateException("init only possible on client side, server does this automatically, use closeWith or answerClose");
        if(step != -2) throw new IllegalStateException("cannot init twice");
        step = 0;
        answerClose(encoded, true);
    }





    //PAUSE

    @Override public P2LMessage initExpectAfterPause(MessageEncoder encoded, int timeout) throws IOException {
        if(isServer) throw new IllegalStateException("not client (server init is automatic, use 'answerExpect')");
        if(step != -2) throw new IllegalStateException("cannot init twice");
        step = 0;
        return answerExpectAfterPause(encoded, timeout, true);
    }
    @Override public P2LMessage answerExpectAfterPause(MessageEncoder encoded, int timeout) throws IOException {
        return answerExpectAfterPause(encoded, timeout, false);
    }
    private P2LMessage answerExpectAfterPause(MessageEncoder encoded, int timeout, boolean init) throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        pausedMode = false;
        //does not request a receipt, but expects to receive a receipt. - resends shall not trigger automatic receipt resends(compare to close, where that is desired)
        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, encoded);

        long stepBeganAt = System.currentTimeMillis();

        for(int i=0;i<maxAttempts;i++) {
            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
            parent.sendInternalMessage(peer, msg);

            pausedDoubleExpectMode = true;
            boolean messageAvailable = SyncHelp.waitUntil(this, () -> lastMessageReceived != null && (lastMessageReceived.header.getStep() == step || lastMessageReceived.header.getStep() == step+1), calcWaitBeforeRetryTime(i));

            if (messageAvailable && (
                    (lastMessageReceived.header.getStep() == step && lastMessageReceived.validateIsReceiptFor(msg))
                    || lastMessageReceived.header.getStep() == step+1)) {
                int adjustedTimeout = (int) (timeout - (System.currentTimeMillis() - stepBeganAt));
                if(adjustedTimeout <= 0) adjustedTimeout = 1;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();

                pausedDoubleExpectMode = false;
                messageAvailable = SyncHelp.waitUntil(this, () -> lastMessageReceived != null && lastMessageReceived.header.getStep() == step, adjustedTimeout);

                step++;

                if(! messageAvailable) {
                    handler.remove(this);
                    throw new TimeoutException();
                }

                //cannot update avRTT, since we have no idea when this result message was sent on the other side(somewhere between lastMessageSent and now)
                DebugStats.conversation_numValid.getAndIncrement();

                return lastMessageReceived;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        handler.remove(this);
        throw new TimeoutException();
    }

    @Override public void pause() throws IOException {
        if(step == -2) throw new IllegalStateException("please init");
        if(step == -1) throw new IllegalStateException("already closed");
        pausedMode = true;
        parent.sendInternalMessage(peer, lastMessageReceived.createReceipt());
    }

















    //SAME STUFF, BUT ASYNC IMPLEMENTATIONS
    @Override public P2LFuture<P2LMessage> initExpectAsync(MessageEncoder encoded)  {
        if(isServer) throw new IllegalStateException("not client (server init is automatic, use 'answerExpect')");
        if(step != -2) throw new IllegalStateException("cannot init twice");
        step = 0;
        return answerExpectAsync(encoded, true);
    }
    @Override public P2LFuture<P2LMessage> answerExpectAsync(MessageEncoder encoded) {
        return answerExpectAsync(encoded, false);
    }
    private P2LFuture<P2LMessage> answerExpectAsync(MessageEncoder encoded, boolean init) {
        return null;
//        if(step == -2) throw new IllegalStateException("please init");
//        if(step == -1) throw new IllegalStateException("already closed");
//
//        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, false);
//        P2LMessage msg = P2LMessage.from(header, encoded);
//
//        step++;
//
//        Retryer<P2LMessage> stepHandler = new Retryer<>();
//        stepHandler.makeAttemptFunc = () -> {
//            //previous is the message we are currently answering to - so it has already been received - however it might be resend by the peer if the message sent below is lost
//            //   it is important the message is handled so that the conversation handler sees that it is expected and does not initiate a new conversation on m0 resend..
//            stepHandler.previous = init ? null : conversationQueue.futureFor(peer, type, conversationId, (short) 0);
//            P2LFuture<P2LMessage> next = conversationQueue.futureFor(peer, type, conversationId, step);
//
//            next.callMeBack(calcWaitBeforeRetryTime(stepHandler.counter), stepHandler);
//            // HAS to be before send message,
//                                                                                        // otherwise the message could be available at the time we call this callback
//                                                                                        // then it would be executed on the same thread
//                                                                                        // - if that occurs twice in a async call cascade this creates a deadlock SOMEWHERE (around finalResultFuture.setCompleted)
//
//            parent.sendInternalMessage(peer, msg);
//            lastMessageSentAt = System.currentTimeMillis();
//        };
//        stepHandler.resultFunc = result -> {
//            if (stepHandler.previous != null) {
//                boolean wasCompleted = ! stepHandler.previous.cancelIfNotCompleted();
//                if (wasCompleted)
//                    DebugStats.conversation_numDoubleReceived.getAndIncrement();
//            }
//            if (result != null) {
//                lastMessageReceived = result;
//                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
//                step++;
//                DebugStats.conversation_numValid.getAndIncrement();
//
//                stepHandler.finalResultFuture.setCompleted(result);
//                return true;
//            } else {
//                DebugStats.conversation_numRetries.getAndIncrement();
//            }
//            return false;
//        };
//
//        stepHandler.makeAttempt();
//
//        return stepHandler.finalResultFuture;
    }


    @Override public P2LFuture<Boolean> answerCloseAsync(MessageEncoder encoded) {
        return answerCloseAsync(encoded, false);
    }
    private P2LFuture<Boolean> answerCloseAsync(MessageEncoder encoded, boolean init) {
        return null;
//        if(step == -2) throw new IllegalStateException("please init");
//        if(step == -1) throw new IllegalStateException("already closed");
//
//        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, true);
//        P2LMessage msg = P2LMessage.from(header, encoded);
//
//        step++;
//
//        Retryer<Boolean> stepHandler = new Retryer<>();
//        stepHandler.makeAttemptFunc = () -> {
//            stepHandler.previous = init?null:conversationQueue.futureFor(peer, type, conversationId, (short) 0);
//            P2LFuture<P2LMessage> last = conversationQueue.receiptFutureFor(peer, type, conversationId, (short) (step - 1));
//
//            last.callMeBack(calcWaitBeforeRetryTime(stepHandler.counter), stepHandler); //HAS TO BE BEFORE SEND MESSAGE - For thread, deadlock and call cascade reasons
//
//            parent.sendInternalMessage(peer, msg);
//            lastMessageSentAt = System.currentTimeMillis();
//        };
//        stepHandler.resultFunc = result -> {
//            if (stepHandler.previous != null) {
//                boolean wasCompleted = !stepHandler.previous.cancelIfNotCompleted();
//                if (wasCompleted)
//                    DebugStats.conversation_numDoubleReceived.getAndIncrement();
//            }
//            if (result != null && result.validateIsReceiptFor(msg)) {
//                lastMessageReceived = result;
//                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
//                step = -1;
//                DebugStats.conversation_numValid.getAndIncrement();
//
//                stepHandler.finalResultFuture.setCompleted(true);
//                return true;
//            } else {
//                DebugStats.conversation_numRetries.getAndIncrement();
//            }
//            return false;
//        };
//
//        stepHandler.makeAttempt();
//        return stepHandler.finalResultFuture;
    }

    @Override public P2LFuture<P2LMessage> initExpectAsyncAfterPause(MessageEncoder encoded, int timeout) {
        if(isServer) throw new IllegalStateException("not client (server init is automatic, use 'answerExpect')");
        if(step != -2) throw new IllegalStateException("cannot init twice");
        step = 0;
        return answerExpectAsyncAfterPause(encoded, timeout, true);
    }
    @Override public P2LFuture<P2LMessage> answerExpectAsyncAfterPause(MessageEncoder encoded, int timeout) {
        return answerExpectAsyncAfterPause(encoded, timeout, false);
    }
    private P2LFuture<P2LMessage> answerExpectAsyncAfterPause(MessageEncoder encoded, int timeout, boolean init)  {
        return null;
//        if(step == -2) throw new IllegalStateException("please init");
//        if(step == -1) throw new IllegalStateException("already closed");
//
//        //does not request a receipt, but expects to receive a receipt. - resends shall not trigger automatic receipt resends(compare to close, where that is desired)
//        ConversationHeader header = new ConversationHeader(null, type, conversationId, step, true);
//        //todo -
//        // requesting a receipt here is technically wrong.
//        // this side can not distinguish between the other side having ended the conversation
//        //        (which would be a dev. bug) and a pause. However. For correct conversations this is correct.
//        P2LMessage msg = P2LMessage.from(header, encoded);
//
//        step++;
//
//        long stepBeganAt = System.currentTimeMillis();
//
//        Retryer<P2LMessage> stepHandler = new Retryer<>();
//        stepHandler.makeAttemptFunc = () -> {
//            stepHandler.previous = init?null:conversationQueue.futureFor(peer, type, conversationId, (short) 0);
//            P2LFuture<P2LMessage> ack = conversationQueue.receiptFutureFor(peer, type, conversationId, (short) (step - 1));
//
//            ack.callMeBack(calcWaitBeforeRetryTime(stepHandler.counter), stepHandler); //HAS TO BE BEFORE SEND MESSAGE - For thread, deadlock and call cascade reasons
//
//            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
//            parent.sendInternalMessage(peer, msg); //this message does NOTHING if the remote has already sent its pause package and is running its long running operation.. How do we get it to resend its pause ack package??
//        };
//        stepHandler.resultFunc = result -> {
//            if (stepHandler.previous != null) {
//                boolean wasCompleted = !stepHandler.previous.cancelIfNotCompleted();
//                if (wasCompleted)
//                    DebugStats.conversation_numDoubleReceived.getAndIncrement();
//            }
//            if (result != null && result.validateIsReceiptFor(msg)) {
//                lastMessageReceived = result;
//                int adjustedTimeout = (int) (timeout - (System.currentTimeMillis() - stepBeganAt));
//                if(adjustedTimeout <= 0) adjustedTimeout = 1;
//                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
//                step++;
//                DebugStats.conversation_numValid.getAndIncrement();
//
//                P2LFuture<P2LMessage> next = conversationQueue.futureFor(peer, type, conversationId, (short) (step - 1));
//
//                next.callMeBack(adjustedTimeout, actualMessage -> { //will throw the same timeout exception timeout is reached, -1 means timeout is never reached
//                    lastMessageReceived = actualMessage;
//                    //cannot update avRTT, since we have no idea when this result message was sent on the other side(somewhere between lastMessageSent and now)
//                    DebugStats.conversation_numValid.getAndIncrement();
//
//                    stepHandler.finalResultFuture.setCompleted(actualMessage);
//                });
//                return true;
//            } else {
//                DebugStats.conversation_numRetries.getAndIncrement();
//            }
//            return false;
//        };
//
//        stepHandler.makeAttempt();
//        return stepHandler.finalResultFuture;
    }


    private class Retryer<T> implements Consumer<P2LMessage> {
        private final P2LFuture<T> finalResultFuture = new P2LFuture<>();

        private int counter = 0;
        private P2LFuture<P2LMessage> previous;
        private ResultHandler resultFunc;
        private RunnableThatThrowsIOException makeAttemptFunc;

        @Override public void accept(P2LMessage msg) {
            try {

                boolean success = resultFunc.handle(msg);

                if(! success/* && !finalResultFuture.isLikelyInactive()*/) {
                    if (counter < maxAttempts) {
                        makeAttempt();
                    } else {
                        finalResultFuture.cancel();
                    }

                    counter++;
                }

            } catch (IOException e) {
                fail(e);
            }
        }

        public void makeAttempt() {
            try {
                makeAttemptFunc.run();
            } catch (IOException e) {
                fail(e);
            }
        }

        private void fail(IOException e) {
            e.printStackTrace();
            //todo - how do we handle? - is this sufficient?!:
            finalResultFuture.cancel();
        }
    }

    interface RunnableThatThrowsIOException {
        void run() throws IOException;
    }
    interface ResultHandler {
        /**@return whether the result was accepted */
        boolean handle(P2LMessage result) throws IOException;
    }
}