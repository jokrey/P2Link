package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.ConversationHeader;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentInputStream;
import jokrey.utilities.network.link2peer.node.stream.P2LFragmentOutputStream;
import jokrey.utilities.network.link2peer.util.AsyncRetryer;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * todo For few dropped packages the number of double resend packages is relatively high
 * todo Problem: Consider packets m0 and m1 and m2. client sends m0, server received m0 and sends m1, m1 is lost.
 * todo          Since m1 is the ack for m0 -> client resends m0, roughly exactly when m0 arrieves the server realizes that it has not received m2 and resends m1.
 * todo          Despite only m1 being lost m0 is resent
 * todo   This problem may very well be unsolvable, but it should be kept in mind
 * todo     - OPTION: it could be possible to have the server never resend, UNLESS the client resends...
 *
 * todo - long message intermediate receipts / stream usage for long messages
 */
public class P2LConversationImplV2 implements P2LConversation {
    private static final short STEP_NOT_INITIATED = Short.MIN_VALUE;

    public static int DEFAULT_NUM_ATTEMPTS = 4;
    public static float DEFAULT_M = 1.25f;
    public static int DEFAULT_A = 25;
    public static int DEFAULT_rM = 100;

    public final P2LNodeInternal parent;
    public final ConversationHandler handler;
    private final P2LConnection con;
    public final InetSocketAddress peer;
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
    public P2LConversationImplV2(P2LNodeInternal parent, ConversationHandler handler,
                                 P2LConnection con, InetSocketAddress peer, short type, short conversationId) {
        //SHALL NOT HAVE SIDE EFFECTS, BECAUSE THE CONVERSATION MIGHT BE DESTROYED INSTANTLY
        this.parent = parent;
        this.handler = handler;
        this.peer = peer;
        this.type = type;
        this.conversationId = conversationId;
        if(con == null)
            this.con = new P2LConnection(null, peer, 1024, -1);
        else
            this.con = con;

        headerSize = new ConversationHeader(type, conversationId, step, false).getSize();
    }

    @Override public InetSocketAddress getPeer() { return peer; }
    @Override public short getConversationId() { return conversationId; }
    @Override public short getType() { return type; }
    @Override public int getAvRTT() { return con.avRTT; }
    @Override public int getHeaderSize() { return headerSize; }
    @Override public int getMaxPayloadSizePerPackage() {
        return con.remoteBufferSize - headerSize;
    }

    private int calcWaitBeforeRetryTime(int numPreviousRetries) {
        return ( con.avRTT == -1? 500 :(int) (con.avRTT * m + a) ) + rM*numPreviousRetries;
    }



    private short step = STEP_NOT_INITIATED;
    private boolean pausedMode = false;
    private boolean pausedDoubleExpectMode = false;

    boolean isServer = false;
    public void notifyServerInit(ConversationAnswererChangeThisName handler, ReceivedP2LMessage m0) throws IOException {
        if(! isInitiated()) {
            step = 1;
            isServer = true;
            latest.setCompleted(m0);

            handler.converse(this, m0);
        } else {
            DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }

    private long lastMessageSentAt = 0;
    private P2LFuture<ReceivedP2LMessage> latest = new P2LFuture<>();
    public void notifyReceived(ReceivedP2LMessage received) throws IOException {
        if(pausedMode) {
            parent.sendInternalMessage(peer, received.createReceipt());
        }
        if(received.header.getStep() == step || (pausedDoubleExpectMode && received.header.getStep() == step+1)) {
            latest.trySetCompleted(received);
        } else {
            System.out.println(parent.getSelfLink()+" - received unexpected message step: "+received.header.getStep()+", expected: "+step);
            DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }

    private boolean isInitiated() {return step != STEP_NOT_INITIATED;}
    private boolean isClosed() {return step < 0 && isInitiated();}
    private void initIfPossible() {
        if(isServer) throw new IllegalStateException("not client (server init is automatic, use 'answerExpect')");
        if(isInitiated()) throw new IllegalStateException("cannot init twice");
        step = 0;
    }
    private void ensureCanStep() {
        if(! isInitiated()) throw new IllegalStateException("please init");
        if(isClosed()) throw new IllegalStateException("already closed");
    }
    private void ensureCanStepWith(MessageEncoder message) {
        ensureCanStep();
        if (message.contentSize() > con.remoteBufferSize * 3) System.err.println("message requires more than three packages - should use long message functionality");
    }
    private void resetLatest() {
        latest.tryCancel();
        latest = new P2LFuture<>();
    }
    private void markSelfClosed() {
        if(isInitiated() && !isClosed()) {
            step = (short) -step;
            handler.remove(this);
        } else {
            throw new IllegalStateException("already closed or not yet initiated");
        }
    }



    //ONLY ON CLIENT as first convo call, requires an answerExpect on the other side after
    @Override public ReceivedP2LMessage initExpect(MessageEncoder message) throws IOException {
        initIfPossible();
        return answerExpect(message);
    }
    //requires and answerExpect or initExpect on the other side before, and an answerExpect or answerClose after
    @Override public ReceivedP2LMessage answerExpect(MessageEncoder message) throws IOException {
        ensureCanStepWith(message);
        pausedMode = false;

        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        step++;

        resetLatest();
        for(int i=0;i<maxAttempts;i++) {
            parent.sendInternalMessage(peer, msg);
            lastMessageSentAt = System.currentTimeMillis();

            ReceivedP2LMessage result = latest.getOrNull(calcWaitBeforeRetryTime(i));

            if (result != null) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();
                return result;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        throw new TimeoutException("after "+maxAttempts+" attempts");
    }


    //REGULAR CLOSE

    //NOTE: KNOWN UNSOLVABLE PROBLEM:
    //      If the receipt package for close is lost, the other side will retry requesting a new receipt.
    //      Both packages have to make it through, since only the answerClose side is retrying now...

    //requires an answerClose on the other side
    @Override public void close() throws IOException {
        ensureCanStep();
        //if this message is lost answerClose will resent, but the message will not be handled by a handler - requestReceipt hits and a receipt equal to this will be sent
        parent.sendInternalMessage(peer, latest.getResult().createReceipt());
        markSelfClosed();
    }

    //requires a close on the other side
    @Override public void answerClose(MessageEncoder message) throws IOException {
        ensureCanStepWith(message);
        pausedMode = false;
        //always requests receipt, but only receives it through that if the remote was not waiting for it. Otherwise answerExpect returns and sends close
        ConversationHeader header = new ConversationHeader(type, conversationId, step, true);
        P2LMessage msg = P2LMessage.from(header, message);

        resetLatest();
        for(int i=0;i<maxAttempts;i++) {
            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
            parent.sendInternalMessage(peer, msg);

            ReceivedP2LMessage result = latest.getOrNull(calcWaitBeforeRetryTime(i));

            if (result != null && result.validateIsReceiptFor(msg)) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                markSelfClosed();
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
    @Override public ReceivedP2LMessage initExpectClose(MessageEncoder message) throws IOException {
        if(isInitiated()) throw new IllegalStateException("cannot init twice");
        step = 0;
        ReceivedP2LMessage result = answerExpect(message);
        markSelfClosed();
        return result;
    }
    @Override public P2LFuture<ReceivedP2LMessage> initExpectCloseAsync(MessageEncoder message) {
        if(isInitiated()) throw new IllegalStateException("cannot init twice");
        step = 0;
        P2LFuture<ReceivedP2LMessage> result = answerExpectAsync(message);
        result.whenCanceled(() -> Thread.dumpStack());
        result.callMeBackFirst(received -> markSelfClosed());
        return result;
    }
    @Override public void closeWith(MessageEncoder message) throws IOException {
        if(!isServer) throw new IllegalStateException("can only be used when server");
        if(step != 1) throw new IllegalStateException("can only be used as first instruction");
        if(message.contentSize() > con.remoteBufferSize*3) System.err.println("message requires more than three packages - should use long message functionality");
        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);
        markSelfClosed();
        parent.sendInternalMessage(peer, msg);
    }
    @Override public void initClose(MessageEncoder message) throws IOException {
        if(isServer) throw new IllegalStateException("init only possible on client side, server does this automatically, use closeWith or answerClose");
        if(isInitiated()) throw new IllegalStateException("cannot init twice");
        step = 0;
        answerClose(message);
    }
    @Override public P2LFuture<Boolean> initCloseAsync(MessageEncoder message) {
        if(isServer) throw new IllegalStateException("init only possible on client side, server does this automatically, use closeWith or answerClose");
        if(isInitiated()) throw new IllegalStateException("cannot init twice");
        step = 0;
        return answerCloseAsync(message);
    }



    //PAUSE

    @Override public ReceivedP2LMessage initExpectAfterPause(MessageEncoder message, int timeout) throws IOException {
        initIfPossible();
        return answerExpectAfterPause(message, timeout);
    }
    @Override public ReceivedP2LMessage answerExpectAfterPause(MessageEncoder message, int timeout) throws IOException {
        ensureCanStepWith(message);
        pausedMode = false;
        //does not request a receipt, but expects to receive a receipt. - resends shall not trigger automatic receipt resends(compare to close, where that is desired)
        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        long stepBeganAt = System.currentTimeMillis();

        pausedDoubleExpectMode = true;
        resetLatest();
        for(int i=0;i<maxAttempts;i++) {
            lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT
            parent.sendInternalMessage(peer, msg);

            ReceivedP2LMessage result = latest.getOrNull(calcWaitBeforeRetryTime(i));

            if(result != null && (
                    (result.header.getStep() == step && result.validateIsReceiptFor(msg))
                    || result.header.getStep() == step+1)) {
                int adjustedTimeout = (int) (timeout - (System.currentTimeMillis() - stepBeganAt));
                if(adjustedTimeout <= 0) adjustedTimeout = 1;
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();

                pausedDoubleExpectMode = false;
                if(result.header.getStep() != step) {
                    latest = new P2LFuture<>();
                    result = latest.getOrNull(adjustedTimeout);
                }

                step++;

                if(result == null) {
                    handler.remove(this);
                    throw new TimeoutException();
                }

                //cannot update avRTT, since we have no idea when this result message was sent on the other side(somewhere between lastMessageSent and now)
                DebugStats.conversation_numValid.getAndIncrement();

                return result;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
        }

        handler.remove(this);
        throw new TimeoutException();
    }

    @Override public void pause() throws IOException {
        ensureCanStep();
        pausedMode = true;
        parent.sendInternalMessage(peer, latest.get().createReceipt());
    }





    //STREAMED MESSAGE IMPLEMENTATION - BLOCKING

    @Override public void initExpectLong(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException {
        initIfPossible();
        answerExpectLong(message, messageTarget, timeout);
    }
    @Override public void answerExpectLong(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException {
        ensureCanStep();
        pausedMode = false;

        long stepBeganAt = System.currentTimeMillis();

        P2LFragmentInputStream in = parent.createFragmentInputStream(peer, type, conversationId, (short) (step+1));
        in.writeResultsTo(messageTarget);
        P2LFuture<Boolean> receivedAnyResponse = new P2LFuture<>();
        in.addFragmentReceivedListener((fragmentOffset, receivedRaw, dataOff, dataLen, eof) -> receivedAnyResponse.trySetCompleted(true));

        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        step++;

        for(int i=0;i<maxAttempts;i++) {
            parent.sendInternalMessage(peer, msg);
            Boolean result = receivedAnyResponse.getOrNull(calcWaitBeforeRetryTime(i));
            if(result!=null) {
                latest.tryCancel();
                latest = new P2LFuture<>(new ConversationHeader(type, conversationId, step, false).generateMockReceived(getPeer()));//in case the next message is close..
                int adjustedTimeout = (int) (timeout - (System.currentTimeMillis() - stepBeganAt));
                if(adjustedTimeout <= 0) adjustedTimeout = 1;
                boolean success = in.waitForFullyReceived(adjustedTimeout);
                step++;
                if(success) {
                    in.close();
                    System.out.println("success = " + success);
                    return;
                } else break;
            }
        }

        in.close();
        handler.remove(this);
        throw new TimeoutException();
    }

    @Override public ReceivedP2LMessage longAnswerExpect(TransparentBytesStorage messageSource, int timeout) throws IOException {
        ensureCanStep();
        pausedMode = false;
        long stepBeganAt = System.currentTimeMillis();
        P2LFragmentOutputStream out = parent.createFragmentOutputStream(peer, type, conversationId, step); //not using step. Should not be a problem, maybe.
        out.setSource(messageSource);

        step++;
        resetLatest();

        out.send();
        boolean success = out.close(timeout);
        if(success) {
            int elapsed = (int) (System.currentTimeMillis() - stepBeganAt);
            int adjustedTimeout = timeout - elapsed;
            if (adjustedTimeout > 0 || latest.isCompleted()) {
                ReceivedP2LMessage result = latest.getOrNull(adjustedTimeout); //todo - technically this message could/should be included in the last receipt - would require 1 packet less.
                if (result != null) {
                    step++;
                    return result;
                }
            }
        }

        handler.remove(this);
        throw new TimeoutException();
    }

    @Override public void longAnswerClose(TransparentBytesStorage messageSource, int timeout) throws IOException {
        ensureCanStep();
        pausedMode = false;
        long ctmUpTop = System.currentTimeMillis();
        P2LFragmentOutputStream out = parent.createFragmentOutputStream(peer, type, conversationId, step); //not using step. Should not be a problem, maybe.
        out.setSource(messageSource);

        resetLatest();

        out.send();
        boolean success = out.close(timeout);
        if(success) {
            int elapsed = (int) (System.currentTimeMillis() - ctmUpTop);
            int adjustedTimeout = timeout - elapsed;
            if (adjustedTimeout > 0 || latest.isCompleted()) {
                System.out.println("waiting for: step = " + step);
//                ReceivedP2LMessage result = latest.getOrNull(adjustedTimeout); //NOT REQUIRED, BECAUSE THE STREAM HAD A RECEIPT WHICH IS SUFFICIENT - OTHER SIDE DOES NOT NEED TO SEND CLOSE (but it doesn't hurt too much)
//                if (result != null) {
                markSelfClosed();
                return;
//                }
            }
        }

        handler.remove(this);
        throw new TimeoutException();
    }
    @Override public void longAnswerExpectLong(TransparentBytesStorage messageSource, TransparentBytesStorage messageTarget, int timeout) throws IOException {
        throw new UnsupportedOperationException("cannot optimize - fragment stream does not yet offer this functionality - if it is strictly required do PR");
    }

    @Override public void initExpectLongAfterPause(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException {
        initIfPossible();
        answerExpectLongAfterPause(message, messageTarget, timeout);
    }

    @Override public void answerExpectLongAfterPause(MessageEncoder message, TransparentBytesStorage messageTarget, int timeout) throws IOException {
        ensureCanStep();
        pausedMode = false;
        long stepBeganAt = System.currentTimeMillis();
        P2LFragmentInputStream in = parent.createFragmentInputStream(peer, type, conversationId, (short) (step+1));
        in.writeResultsTo(messageTarget);

        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        resetLatest();
        for(int i=0;i<maxAttempts;i++) {
            parent.sendInternalMessage(peer, msg);

            ReceivedP2LMessage result = latest.getOrNull(calcWaitBeforeRetryTime(i));
            if(result!=null) {
                int adjustedTimeout = (int) (timeout - (System.currentTimeMillis() - stepBeganAt));
                if(adjustedTimeout <= 0) adjustedTimeout = 1;
                boolean success = in.waitForFullyReceived(adjustedTimeout);
                step++;
                latest = new P2LFuture<>(new ConversationHeader(type, conversationId, step, false).generateMockReceived(getPeer()));//in case the next message is close..
                step++;
                if(success) {
                    in.close();
                    System.out.println("success = " + success+", step="+step);
                    return;
                } else break;
            }
        }

        in.close();
        handler.remove(this);
        throw new TimeoutException();
    }









    //SHORT MESSAGES, BUT ASYNC IMPLEMENTATIONS

    @Override public P2LFuture<ReceivedP2LMessage> initExpectAsync(MessageEncoder message)  {
        initIfPossible();
        return answerExpectAsync(message);
    }
    @Override public P2LFuture<ReceivedP2LMessage> answerExpectAsync(MessageEncoder message) {
        ensureCanStepWith(message);
        pausedMode = false;

        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        step++;

        latest.tryCancel();
        DefaultRetryer<ReceivedP2LMessage> stepHandler = new DefaultRetryer<>(msg);
        stepHandler.resultFunc = result -> {
            if (result != null) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();

                return result;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
            }
            return null;
        };

        stepHandler.start();

        return stepHandler.finalResultFuture;
    }


    @Override public P2LFuture<Boolean> answerCloseAsync(MessageEncoder message) {
        ensureCanStepWith(message);
        pausedMode = false;

        ConversationHeader header = new ConversationHeader(type, conversationId, step, true);
        P2LMessage msg = P2LMessage.from(header, message);

        latest.tryCancel();
        DefaultRetryer<Boolean> stepHandler = new DefaultRetryer<>(msg);
        stepHandler.resultFunc = result -> {
            if (result != null && result.validateIsReceiptFor(msg)) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                markSelfClosed();
                DebugStats.conversation_numValid.getAndIncrement();

                return true;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
                return null;
            }
        };

        stepHandler.start();
        return stepHandler.finalResultFuture;
    }

    @Override public P2LFuture<ReceivedP2LMessage> initExpectAsyncAfterPause(MessageEncoder message) {
        initIfPossible();
        return answerExpectAsyncAfterPause(message);
    }
    @Override public P2LFuture<ReceivedP2LMessage> answerExpectAsyncAfterPause(MessageEncoder message) {
        ensureCanStepWith(message);
        pausedMode = false;

        //does not request a receipt, but expects to receive a receipt. - resends shall not trigger automatic receipt resends(compare to close, where that is desired)
        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        pausedDoubleExpectMode = true;
        latest.tryCancel();
        DefaultRetryer<ReceivedP2LMessage> stepHandler = new DefaultRetryer<>(msg);
        stepHandler.resultFunc = result -> {
            if (result != null && (
                    (result.header.getStep() == step && result.validateIsReceiptFor(msg))
                            || result.header.getStep() == step+1)) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                DebugStats.conversation_numValid.getAndIncrement();

                pausedDoubleExpectMode = false;
                if(result.header.getStep() != step) {
                    latest = new P2LFuture<>();
                    latest.callMeBack(actualMessage -> { //will throw the same timeout exception timeout is reached, -1 means timeout is never reached
                        //cannot update avRTT, since we have no idea when this result message was sent on the other side(somewhere between lastMessageSent and now)
                        if(actualMessage == null)
                            stepHandler.finalResultFuture.cancel();
                        else {
                            step++;
                            DebugStats.conversation_numValid.getAndIncrement();
                            stepHandler.finalResultFuture.setCompleted(actualMessage);
                        }
                    });
                    stepHandler.stopButDoNotComplete = true;
                    return null;
                } else {
                    step++;
                    return result;
                }
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
                return null;
            }
        };

        stepHandler.start();
        return stepHandler.finalResultFuture;
    }





    //STREAMED MESSAGE IMPLEMENTATION - ASYNC

    @Override public P2LFuture<Boolean> initExpectLongAsync(MessageEncoder message, TransparentBytesStorage messageTarget) {
        initIfPossible();
        return answerExpectLongAsync(message, messageTarget);
    }
    @Override public P2LFuture<Boolean> answerExpectLongAsync(MessageEncoder message, TransparentBytesStorage messageTarget) {
        ensureCanStep();
        pausedMode = false;

        P2LFragmentInputStream in = parent.createFragmentInputStream(peer, type, conversationId, (short) (step+1));
        in.writeResultsTo(messageTarget);
        P2LFuture<Boolean> receivedAnyResponse = new P2LFuture<>();
        in.addFragmentReceivedListener((fragmentOffset, receivedRaw, dataOff, dataLen, eof) -> receivedAnyResponse.trySetCompleted(true));

        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        step++;

        latest.tryCancel();
        DefaultRetryer<Boolean> stepHandler = new DefaultRetryer<>(msg);//todo - should be a different retryer that does not use latest
        stepHandler.resultFunc = result -> {
            if(receivedAnyResponse.isCompleted()) {
                latest.tryCancel();
                latest = new P2LFuture<>(new ConversationHeader(type, conversationId, step, false).generateMockReceived(getPeer()));//in case the next message is close..

                step++;
                return true;
            }
            return null;
        };

        stepHandler.start();
        return stepHandler.finalResultFuture.andThen(success -> {
            if(success == null || !success)
                return P2LFuture.canceled();
            else
                return in.createAllReceivedFuture();
        });
    }

    @Override public P2LFuture<ReceivedP2LMessage> longAnswerExpectAsync(TransparentBytesStorage messageSource) {
        ensureCanStep();
        pausedMode = false;
        P2LFragmentOutputStream out = parent.createFragmentOutputStream(peer, type, conversationId, step); //not using step. Should not be a problem, maybe.
        out.setSource(messageSource);

        step++;
        resetLatest();

        P2LFuture<Boolean> streamCloseFuture = out.closeAsync();//P2LThreadPool.executeSingle(out::close);
        streamCloseFuture.callMeBack(success -> {
            if(success == null) { //stream canceled...
                handler.remove(P2LConversationImplV2.this);
            }
        });

        return streamCloseFuture.andThen(success -> {
            if(success) {
                latest.callMeBackFirst(message -> {//important that called first, otherwise the step might not be incremented but the callback in the user already calls the next method.
                    if (message == null) handler.remove(this);   //canceled
                    else step++; //step has to be done AFTER receiving latest....
                });
                return latest;
            }
            return P2LFuture.canceled();
        });
    }

    @Override public P2LFuture<Boolean> longAnswerCloseAsync(TransparentBytesStorage messageSource) {
        ensureCanStep();
        pausedMode = false;
        P2LFragmentOutputStream out = parent.createFragmentOutputStream(peer, type, conversationId, step); //not using step. Should not be a problem, maybe.
        out.setSource(messageSource);

        resetLatest();

        out.send();
        markSelfClosed();
        return out.closeAsync();//todo - it might be more efficient to spawn a thread here than to use what close Async uses internally.....
    }
    @Override public P2LFuture<Boolean> longAnswerExpectLongAsync(TransparentBytesStorage messageSource, TransparentBytesStorage messageTarget) {
        throw new UnsupportedOperationException("cannot optimize - fragment stream does not yet offer this functionality - if it is strictly required do PR");
    }

    @Override public P2LFuture<Boolean> initExpectLongAsyncAfterPause(MessageEncoder message, TransparentBytesStorage messageTarget) {
        initIfPossible();
        return answerExpectLongAsyncAfterPause(message, messageTarget);
    }

    @Override public P2LFuture<Boolean> answerExpectLongAsyncAfterPause(MessageEncoder message, TransparentBytesStorage messageTarget) {
        ensureCanStepWith(message);
        pausedMode = false;
        P2LFragmentInputStream in = parent.createFragmentInputStream(peer, type, conversationId, (short) (step+1));
        in.writeResultsTo(messageTarget);

        //does not request a receipt, but expects to receive a receipt. - resends shall not trigger automatic receipt resends(compare to close, where that is desired)
        ConversationHeader header = new ConversationHeader(type, conversationId, step, false);
        P2LMessage msg = P2LMessage.from(header, message);

        latest.tryCancel();
        DefaultRetryer<Boolean> stepHandler = new DefaultRetryer<>(msg);//todo - should be a different retryer that does not use latest
        stepHandler.resultFunc = result -> {
            if (result != null && result.validateIsReceiptFor(msg)) {
                con.updateAvRTT((int) (System.currentTimeMillis() - lastMessageSentAt));
                step++;
                latest = new P2LFuture<>(new ConversationHeader(type, conversationId, step, false).generateMockReceived(getPeer()));//in case the next message is close..
                step++;
                return true;
            } else {
                DebugStats.conversation_numRetries.getAndIncrement();
                return null;
            }
        };

        stepHandler.start();
        return stepHandler.finalResultFuture.andThen(success -> {
            if(success == null || !success) {
                try { in.close(); } catch (IOException e) { e.printStackTrace(); }
                return P2LFuture.canceled();
            } else
                return in.createAllReceivedFuture();
        });
    }

    private class DefaultRetryer<T> extends AsyncRetryer<T, ReceivedP2LMessage> {
        DefaultRetryer(P2LMessage messageToRetry) {
            super(P2LConversationImplV2.this.maxAttempts);
            finalResultCanceledHandler = () -> handler.remove(P2LConversationImplV2.this);//IF RESULT FUTURE IS CANCELED THE ENTIRE CONVERSATION IS CANCELED.
            attemptFunction = () -> {
                latest = new P2LFuture<>();
                latest.callMeBack(calcWaitBeforeRetryTime(counter), this); //HAS TO BE BEFORE SEND MESSAGE - For thread, deadlock and call cascade reasons

                lastMessageSentAt = System.currentTimeMillis();//used to calculate avRTT later
                parent.sendInternalMessage(peer, messageToRetry); //this message does NOTHING if the remote has already sent its pause package and is running its long running operation.. How do we get it to resend its pause ack package??
            };
        }
    }
}