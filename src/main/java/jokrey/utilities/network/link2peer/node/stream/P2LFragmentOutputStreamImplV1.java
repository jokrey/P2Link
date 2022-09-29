package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.node.stream.fragment.*;
import jokrey.utilities.network.link2peer.util.AsyncCallbackSchedulerThread;
import jokrey.utilities.network.link2peer.util.AsyncLoop;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.simple.data_structure.lists.LongTupleList;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 *
 * Fragment vs Continuous:
 *   All data available without buffering
 *      Can query and resend any slice of data
 *      (Also means that the data should not be altered during sending, or rather the change would be reflected)
 *   Data does not need to be consumed in order
 *      it can be written out instantly in any order (due to offset)
 *   (Fixed length, Fixed chunkSize)  - For now
 *   64bit(32 for now) offset, chunkSize can be read from first package(does not matter which - since fixed)
 *   Length fixed would allow: Requery from receiver side (it knows packages are missing)
 *     After twice av round trip time
 *     Otherwise, the sender would have to resend after twice av round trip time without receipt and send last package
 * ADDITIONALLY REQUIRED:
 *   Delay/max num packages sent simultaneously (defaults to
 *   Based on percentage of received packages (should be above 80% (???))
 *   When to send the next package batch? Av-round-trip-time / 4 (???)
 *   MTU IS FIXED (Due to requirements of buffer size being small to mitigate ddos attack)
 *   Requery/Resend of packages
 *   After twice av round trip time (Defaults to 500,but dynamically adjusted (sliding average - but outlier detection, i.e. not + /2,but instead a slow down change factor)
 *
 * Question: Should sender resend after x ms, or should receiver resendReceipt after x ms
 *    SENDER RESEND!!
 *    Receiver does not necessarily know that something is missing
 *
 * Analyse: empty packages (NOT SURE IF STILL UP TO DATE)
 *   Problematic request receipt for all packages resend after stream is complete
 *   INSTEAD: For all short batches: request receipt with last short batch
 *
 * @author jokrey
 */
public class P2LFragmentOutputStreamImplV1 extends P2LFragmentOutputStream {
    public static LossAcceptabilityCalculator lossConverter = new LossAcceptabilityCalculator_Packages(4);
    public static BatchSizeCalculatorCreator batchSizeCalculatorCreator = new BatchSizeCalculator_StrikeDown(256, 32, 8).creator();

    public static final long DEFAULT_BATCH_DELAY = 333;
    public static final int RECEIPT_DELAY_MULTIPLIER = 3;

    private final int packageSize;
    private final long batch_delay_ms;

    private final BatchSizeCalculator batchSizeCalculator;
    protected P2LFragmentOutputStreamImplV1(P2LNodeInternal parent, InetSocketAddress to, P2LConnection con, short type, short conversationId, short step) {
        super(parent, to, con, type, conversationId, step);
        int headerSize = new StreamPartHeader(type, conversationId, step,0, false, false).getSize();
        packageSize = con==null?1024:con.remoteBufferSize - headerSize;
        batch_delay_ms = con==null? DEFAULT_BATCH_DELAY :Math.max(con.avRTT, DEFAULT_BATCH_DELAY);

        batchSizeCalculator = batchSizeCalculatorCreator.create(con);
    }


    //RUNTIME VARIABLES AND FLAGS
    private long latestConfirmedIndex = 0;
    private long latestSentIndex = 0;
    private boolean allReceived() {
//        System.out.println("source.totalNumBytes() = " + source.totalNumBytes());
//        System.out.println("latestConfirmedIndex = " + latestConfirmedIndex);
        return source.totalNumBytes() == latestConfirmedIndex;
    }

    @Override public void send() {
        if(latestSentIndex == 0) {
            double minTimeInSeconds = (((source.totalNumBytes() / (double) packageSize) / (double) batchSizeCalculator.getBatchSize()) * batch_delay_ms) / 1000.0;
            System.out.println("minTime = " + minTimeInSeconds); //implementation can readily exceed or be better than min time - by adaptively increasing the batch_size - can also be worse in case batch size has to be decreased
        }

        while(latestSentIndex < source.currentMaxEnd()) {
            long newEndIndex = Math.min(source.currentMaxEnd(), latestSentIndex+packageSize);
            addLastInBatch(source.sub(latestSentIndex, newEndIndex));//internally uses sendBatch
            latestSentIndex = newEndIndex;
        }
        sendBatch();

        //not sure why this existed:
//        while(!batch.isEmpty()) {
//            sendBatch();
////            try { Thread.sleep(10); } catch (InterruptedException e) { e.printStackTrace(); }
//        }
    }

    @Override public P2LFuture<Boolean> sendAsync() {
        System.out.println("P2LFragmentOutputStreamImplV1.sendAsync");
        if(latestSentIndex == 0) {
            double minTimeInSeconds = (((source.totalNumBytes() / (double) packageSize) / (double) batchSizeCalculator.getBatchSize()) * batch_delay_ms) / 1000.0;
            System.out.println("minTime = " + minTimeInSeconds); //implementation can readily exceed or be better than min time - by adaptively increasing the batch_size - can also be worse in case batch size has to be decreased
        }

        AsyncLoop<Boolean> loop = new AsyncLoop<>(t -> {
            if(latestSentIndex < source.currentMaxEnd()) {
                long newEndIndex = Math.min(source.currentMaxEnd(), latestSentIndex + packageSize);
                P2LFuture<Boolean> fut = addLastInBatchAsync(source.sub(latestSentIndex, newEndIndex));
                latestSentIndex = newEndIndex;
                return fut;
            }
            return null;
        });

        return loop.andThen(success -> {
            System.out.println("sendAsync.andThen - success = " + success);
            if(success != null && success)
                return sendBatchAsync();
            return P2LFuture.canceled();
        });
    }


    private LongTupleList latestMissingRanges = null;
    private long remoteLatestReceivedDataOffset = -1;
    private int receiptID = 0;

    private final ArrayList<Pair<Long, LongTupleList>> sendSinceLastReceipt = new ArrayList<>(RECEIPT_DELAY_MULTIPLIER+1);

    @Override public synchronized void receivedReceipt(P2LMessage rawReceipt) {
        if (rawReceipt.getPayloadLength() == 0) { //todo remote no longer knows about this stream conversation... This may be valid - or not. Currently we just treat it as correct.
            latestMissingRanges = null;
            remoteLatestReceivedDataOffset = -3;
            SyncHelp.notify(this);
            return;
        }

        P2LFragmentStreamReceipt receipt = P2LFragmentStreamReceipt.decode(rawReceipt);

        if(receipt.receiptID < receiptID && !(receiptID>0 && receipt.receiptID<0) )
            return;
        receiptID=receipt.receiptID;

        latestConfirmedIndex = receipt.missingRanges.isEmpty()?receipt.latestReceived:receipt.missingRanges.get0(0) - 1;
        source.adviceEarliestRequiredIndex(latestConfirmedIndex+1);

        System.out.println("receivedReceipt - receipt.eof = " + receipt.eof+", receipt.latestReceived = " + receipt.latestReceived+", receipt.missingRanges = " + receipt.missingRanges);
        if(receipt.eof) {
            if(source.totalNumBytes() != -1 && receipt.latestReceived >= source.totalNumBytes() && receipt.missingRanges.isEmpty()) {
                latestMissingRanges = null;
                remoteLatestReceivedDataOffset = receipt.latestReceived;
            } else { //input stream premature close
                latestMissingRanges = null;
                remoteLatestReceivedDataOffset=-2;
            }
        } else {
            LongTupleList updatedMissingRanges = receipt.missingRanges;

//            System.out.println("updatedMissingRanges = " + updatedMissingRanges);
//            System.out.println("sendSinceLastReceipt 1 = " + sendSinceLastReceipt);

            long[] recently = calculateRecentlyLost(updatedMissingRanges);
            long recentlyLostBytes = recently[0];
            long recentlySentBytes = recently[1];
//            System.out.println("sendSinceLastReceipt 2 = " + sendSinceLastReceipt);

            latestMissingRanges = updatedMissingRanges;

            LossResult lossResult = lossConverter.calculateAcceptability(recentlyLostBytes, recentlySentBytes, packageSize);
            batchSizeCalculator.adjustBatchSize(lossResult);

            reEnqueueMissingRanges();

//            System.out.println("lossResult = " + lossResult);
            System.out.println("batchSizeCalculator.getBatchSize() = " + batchSizeCalculator.getBatchSize());
//            System.out.println("batch_delay_ms = " + batch_delay_ms);
//            System.out.println("newlyKnownBytesDropped = " + recentlyLostBytes+"/"+totalLost);
//            System.out.println("packageSize = " + packageSize);
        }
        SyncHelp.notify(this);
    }

    private boolean isIn(long r1S, long r1E, long r2S, long r2E) {
        return r1S >= r2S && r1E <= r2E;
    }

    private synchronized void reEnqueueMissingRanges() {
        if(latestMissingRanges==null) return;

        LongTupleList latestMissing = new LongTupleList(latestMissingRanges.size()/2);
//        long totalLostBytes = 0;
        for(int i = 0; i<latestMissingRanges.size(); i++) {
            long rangeS = latestMissingRanges.get0(i);
            long rangeE = latestMissingRanges.get1(i);
//            if(! allowResend(rangeS, rangeE, allSend)) {//A CERTAIN number of packages should be ignored, due to likely or possible reordering of packages by the receiver.. - does not happen often because RECEIPT_DELAY_MULTIPLIER
//                System.out.println("blocked from resend range = [" + rangeS+", "+rangeE+"]");
//                latestMissing.add(rangeS, rangeE);
//                continue;
//            }
//            totalLostBytes+=rangeE-rangeS;

            enqueueRange(rangeS, rangeE);
        }
//        latestMissingRanges=null; //muy importante
        latestMissingRanges=latestMissing;

    }

//    private boolean allowResend(long rangeS, long rangeE, boolean allSend) {
////        return true; //in all honesty this feature seems to have little effect(due to the receipt delay multiplier)
//        return allSend || highestOffsetSentInLastBatch<0 || rangeE < highestOffsetSentInLastBatch-((batchSizeCalculator.getBatchSize())*packageSize);
//    }

    private void enqueueRange(long start, long end) {
        long curStart = start;
        while(curStart < end) {
            DebugStats.fragmentStream1_numResend.getAndIncrement();
            System.out.println("resend("+packageSize+"): ["+curStart+", "+Math.min(end, curStart+packageSize)+"]");
            addFirstInBatch(source.sub(curStart, Math.min(end, curStart+=packageSize)));
        }
        sendBatch();
    }

    private final ConcurrentLinkedDeque<Fragment> batch = new ConcurrentLinkedDeque<>();

    private void addFirstInBatch(Fragment toSend) {
        if(toSend.isEmpty()) return;
        if(!batch.contains(toSend)) //todo maybe slower than it needs to be
            batch.addFirst(toSend);
        waitForBatchCapacity();
//        System.out.println("addFirstInBatch - toSend[" + toSend.realStartIndex+", "+toSend.realEndIndex+"]");
        if(batch.size() >= batchSizeCalculator.getBatchSize())
            sendBatch();
    }
    private void addLastInBatch(Fragment toSend) {
        if(toSend.isEmpty()) return;
//        System.out.println("addLastInBatch - toSend[" + toSend.realStartIndex+", "+toSend.realEndIndex+"]");
        if(!batch.contains(toSend)) //todo maybe slower than it needs to be
            batch.addLast(toSend);
        waitForBatchCapacity();
        if(batch.size() >= batchSizeCalculator.getBatchSize())
            sendBatch();
    }
    private P2LFuture<Boolean> addFirstInBatchAsync(Fragment toSend) {
        if(toSend.isEmpty()) return new P2LFuture<>(true);
        if(!batch.contains(toSend)) //todo maybe slower than it needs to be
            batch.addFirst(toSend);
        return sendBatchWhenPossible();
    }
    private P2LFuture<Boolean> addLastInBatchAsync(Fragment toSend) {
        if(toSend.isEmpty()) return new P2LFuture<>(true);
        if(!batch.contains(toSend)) //todo maybe slower than it needs to be
            batch.addLast(toSend);
        return sendBatchWhenPossible();
    }

    private P2LFuture<Boolean> sendBatchWhenPossible() {
        if(batch.size() >= batchSizeCalculator.getBatchSize()) {
            P2LFuture<Boolean> added = new P2LFuture<>();
            AsyncCallbackSchedulerThread.instance().add((int) calculateTimeToCapacityAllowsNextSend(), () -> {
                sendBatchAsync().callMeBack(success -> {
                    if (success == null)
                        added.cancel();
                    else
                        added.setCompleted(success);
                });
            });
            return added;
        } else
            return new P2LFuture<>(true);
    }

    private long lastBatchSentAt = -1;
    private long highestOffsetSentInLastBatch = -1;
    private int numPackagesSentInBatch = 0;
    private synchronized void sendBatch() {
        try {

//            System.out.println(System.currentTimeMillis()+" sendBatch("+batch.size()+")-("+numPackagesSentInBatch+"/"+batchSizeCalculator.getBatchSize()+"): elapsed = " + (System.currentTimeMillis() - lastBatchSentAt));

            endPackageIfPossible();

            filterBatchOfAlreadyReceivedPackages();

            Fragment packageContent;
            while(canSendPackagesInCurrentBatch() && (packageContent = batch.pollFirst()) != null) {
                P2LMessage message = buildP2LMessage(packageContent);
                numPackagesSentInBatch++;
                highestOffsetSentInLastBatch = Math.max(highestOffsetSentInLastBatch, packageContent.realEndIndex);
                latestSentIndex = Math.max(latestSentIndex, packageContent.realEndIndex);
                parent.sendInternalMessage(to, message);

                LongTupleList current = sendSinceLastReceipt.get(0).r;
                for (int i = current.size() - 1; i >= 0; i--) {
                    if(current.get1(i) == packageContent.realStartIndex) {
                        current.set1(i, packageContent.realEndIndex);
                        current=null;
                        break;
                    }
                }
                if(current != null)
                    current.add(packageContent.realStartIndex, packageContent.realEndIndex);
//                System.out.println("packageContent ["+packageContent.realStartIndex+", "+packageContent.realEndIndex+"]");
                delaySendAppropriately();
                endPackageIfPossible();
            }

//            System.out.println(System.currentTimeMillis()+" sentBatch("+batch.size()+")("+numPackagesSentInBatch+"/"+batchSizeCalculator.getBatchSize()+"): elapsed = " + (System.currentTimeMillis() - lastBatchSentAt)+"\n");

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private P2LFuture<Boolean> sendBatchAsync() {
        return new AsyncLoop<Boolean>(t -> {
            synchronized (P2LFragmentOutputStreamImplV1.this) {
                Fragment packageContent;
                endPackageIfPossible();
                if (t == null) {
                    filterBatchOfAlreadyReceivedPackages();
                }
                if (canSendPackagesInCurrentBatch() && (packageContent = batch.pollFirst()) != null) {
                    P2LMessage message = buildP2LMessage(packageContent);
                    numPackagesSentInBatch++;
                    highestOffsetSentInLastBatch = Math.max(highestOffsetSentInLastBatch, packageContent.realEndIndex);
                    latestSentIndex = Math.max(latestSentIndex, packageContent.realEndIndex);
                    parent.sendInternalMessage(to, message);

                    LongTupleList current = sendSinceLastReceipt.get(0).r;
                    for (int i = current.size() - 1; i >= 0; i--) {
                        if (current.get1(i) == packageContent.realStartIndex) {
                            current.set1(i, packageContent.realEndIndex);
                            current = null;
                            break;
                        }
                    }
                    if (current != null)
                        current.add(packageContent.realStartIndex, packageContent.realEndIndex);
                    return AsyncCallbackSchedulerThread.instance().completeAfter((int) calculateSendDelayTime());
                }
                return null;
            }
        });
    }

    private void filterBatchOfAlreadyReceivedPackages() {
        batch.removeIf(p -> {
            if(p.realEndIndex >= remoteLatestReceivedDataOffset)
                return false;
            for(int i = 0; i<latestMissingRanges.size(); i++) {
                long rangeS = latestMissingRanges.get0(i);
                long rangeE = latestMissingRanges.get1(i);
                if(p.realStartIndex >= rangeS && p.realEndIndex <= rangeE)
                    return false;
            }
            return true;
        });
    }

    private boolean canSendPackagesInCurrentBatch() {
        return numPackagesSentInBatch < batchSizeCalculator.getBatchSize();
    }
    private void endPackageIfPossible() {
        if(lastBatchSentAt==-1 || System.currentTimeMillis() - lastBatchSentAt >= batch_delay_ms) {
//            System.out.println(System.currentTimeMillis() + " P2LFragmentOutputStreamImplV1.endPackageIfPossible - elapsedBefore: "+(System.currentTimeMillis() - lastBatchSentAt));
            lastBatchSentAt = System.currentTimeMillis();
            numPackagesSentInBatch = 0;
            if(sendSinceLastReceipt.isEmpty() || sendSinceLastReceipt.get(0).r.size() > 0)
                sendSinceLastReceipt.add(0, new Pair<>(lastBatchSentAt, new LongTupleList(batchSizeCalculator.getBatchSize())));
        }
        if(sendSinceLastReceipt.isEmpty())
            sendSinceLastReceipt.add(0, new Pair<>(lastBatchSentAt, new LongTupleList(batchSizeCalculator.getBatchSize())));
    }

    private P2LMessage buildP2LMessage(Fragment packageContent) {
        boolean lastPackage = packageContent.realEndIndex == source.totalNumBytes();
//        System.out.println("lastPackage = " + lastPackage);
//        System.out.println("packageContent.asString() = " + new String(packageContent.getContent(), StandardCharsets.UTF_8));
        StreamPartHeader header = new StreamPartHeader(type, conversationId, step, (int) (packageContent.realStartIndex), false, lastPackage);
//        System.out.println("content.length = " + content.length);
        return header.generateMessage(packageContent.content());
    }

    private void waitForBatchCapacity() {
        long timeToWait = calculateTimeToCapacityAllowsNextSend();
        try {
            if(timeToWait>0)
                Thread.sleep(timeToWait);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private long calculateTimeToCapacityAllowsNextSend() {
        if(batch.size() >= batchSizeCalculator.getBatchSize() && numPackagesSentInBatch >= batchSizeCalculator.getBatchSize()) {
            long elapsedInBatchWindow = System.currentTimeMillis() - lastBatchSentAt;
            return batch_delay_ms - elapsedInBatchWindow;
        }
        return 0;
    }

    private long calculateSendDelayTime() {
        long elapsedInBatchWindow = System.currentTimeMillis() - lastBatchSentAt;
        long msLeftInCurrentBatchWindow = batch_delay_ms - elapsedInBatchWindow;
        long curPackageSupposedToBeSentAt = (long) ((batch_delay_ms / (double)batchSizeCalculator.getBatchSize()) * numPackagesSentInBatch);
        long msLeftToSendCurrentPackage = curPackageSupposedToBeSentAt - elapsedInBatchWindow; //send at the end of the package window.. - just like we are supposed to have send the batch batch-delay after the last batch was send..

        if(msLeftInCurrentBatchWindow > 0)
            if(msLeftToSendCurrentPackage > 1)//todo - this may be wrong, but a spin lock feels even more wrong....
                return msLeftToSendCurrentPackage;
        return 0;
    }
    private void delaySendAppropriately() throws InterruptedException {
        long timeToSleep = calculateSendDelayTime();
        if(timeToSleep > 0) Thread.sleep(timeToSleep);
    }

    @Override public boolean waitForConfirmationOnAll(int timeout_ms) {
        return SyncHelp.waitUntil(this, this::isClosed, timeout_ms, batch_delay_ms*RECEIPT_DELAY_MULTIPLIER, () -> {
            if(batch.isEmpty()) {
                if(latestMissingRanges == null || latestMissingRanges.isEmpty()) {
                    DebugStats.fragmentStream1_numResend.getAndIncrement();
                    addFirstInBatch(source.sub(Math.max(0, source.currentMaxEnd() - packageSize), source.currentMaxEnd()));
                } else
                    reEnqueueMissingRanges();
            }
            sendBatch();
        }); // con == null? batch_delay_ms: con.avRTT*3
    }
    private P2LFuture<Boolean> waitForConfirmationOnAllAsync() {
        if(isClosed()) return new P2LFuture<>(remoteLatestReceivedDataOffset != -2);

        AsyncLoop<Boolean> loop = new AsyncLoop<>(t -> {
            if(P2LFragmentOutputStreamImplV1.this.isClosed())
                return null;
            if (batch.isEmpty()) {
                if (latestMissingRanges == null || latestMissingRanges.isEmpty()) {
                    DebugStats.fragmentStream1_numResend.getAndIncrement();
                    return P2LFragmentOutputStreamImplV1.this.addFirstInBatchAsync(source.sub(Math.max(0, source.currentMaxEnd() - packageSize), source.currentMaxEnd()));
                } else {
                    P2LFragmentOutputStreamImplV1.this.reEnqueueMissingRanges();
                }
            }
            return AsyncCallbackSchedulerThread.instance().runAfter((int) (batch_delay_ms * RECEIPT_DELAY_MULTIPLIER), this::sendBatchAsync);
        });

        return loop.toBooleanFuture(success -> success && remoteLatestReceivedDataOffset != -2);
    }

    @Override public boolean close(int timeout_ms) {
        if(allReceived()) return true;
        //ntodo mark as closed... so no packages will be sent anymore - no receipts accepted...
        //ntodo - close not send to other stream...
        if(source.currentMaxEnd() != source.totalNumBytes()) throw new IllegalArgumentException("illegal state for close (currentMaxEnd != totalNumBytes)");

        if(latestSentIndex < source.currentMaxEnd())
            send();
//        else {
//            addFirstInBatch(source.sub(Math.max(0, source.currentMaxEnd() - packageSize), source.currentMaxEnd()));
//            sendBatch();
//        }
        try {
            boolean notTimedOut = waitForConfirmationOnAll(timeout_ms);
            if(notTimedOut) {
                return remoteLatestReceivedDataOffset != -2; //premature close
            }
            return false;
        } finally {
            parent.unregister(this);
        }
    }

    @Override public P2LFuture<Boolean> closeAsync() {
        if(allReceived()) return new P2LFuture<>(true);
        if(source.currentMaxEnd() != source.totalNumBytes()) throw new IllegalArgumentException("illegal state for close (currentMaxEnd != totalNumBytes)");

        P2LFuture<Boolean> sendFuture = null;
        if(latestSentIndex < source.currentMaxEnd())
            sendFuture = sendAsync();
//-//        else {
//-//            addFirstInBatch(source.sub(Math.max(0, source.currentMaxEnd() - packageSize), source.currentMaxEnd()));
//-//            sendFuture = sendBatchAsync();
//-//        }

        P2LFuture<Boolean> combinedFuture;

        if(sendFuture != null)
            combinedFuture = sendFuture.andThen(success -> {
                System.out.println("closeAsync - andthen - success = "+success);
                if(success == null || !success)
                    return P2LFuture.canceled();
                return waitForConfirmationOnAllAsync();
            });
        else
            combinedFuture = waitForConfirmationOnAllAsync();

        combinedFuture.callMeBack(success -> parent.unregister(P2LFragmentOutputStreamImplV1.this));

        return combinedFuture;
    }

    @Override public boolean isClosed() {
        return allReceived() || (remoteLatestReceivedDataOffset == -2) || (remoteLatestReceivedDataOffset == -3);
    }

    public long[] calculateRecentlyLost(LongTupleList updatedMissingRanges) {
        long recentlyLostBytes = 0;
        long recentlySentBytes = 0;

        long now = System.currentTimeMillis();
        for (ListIterator<Pair<Long, LongTupleList>> iterator = sendSinceLastReceipt.listIterator(); iterator.hasNext(); ) {
            Pair<Long, LongTupleList> batchSent = iterator.next();
            for (int i = 0; i < batchSent.r.size(); i++) {
                long sentAt = (long) ((now - batchSent.l) + (batch_delay_ms * (i / (double) batchSent.r.size())));
                long thresholdToExpectReceival = now - (con==null?batch_delay_ms:con.avRTT* 2L);
                if (sentAt > thresholdToExpectReceival || i + 1 == batchSent.r.size()) {
                    batchSent.r.removeRange(0, i+1);
                    if(batchSent.r.isEmpty() && iterator.previousIndex()!=-1)
                        iterator.remove();
                    else
                        iterator.set(new Pair<>(sentAt, batchSent.r));
                    break; //all subsequent also hold
                }

                long sentS = batchSent.r.get0(i);
                long sentE = batchSent.r.get1(i);

                recentlySentBytes += sentE-sentS;

                for (int mi = 0; mi < updatedMissingRanges.size(); mi++) {
                    long missingS = updatedMissingRanges.get0(mi);
                    long missingE = updatedMissingRanges.get1(mi);

                    if(isIn(missingS, missingE, sentS, sentE))
                        recentlyLostBytes += missingE-missingS;
                }
            }
        }
        return new long[] {recentlyLostBytes, recentlySentBytes};
    }
}
