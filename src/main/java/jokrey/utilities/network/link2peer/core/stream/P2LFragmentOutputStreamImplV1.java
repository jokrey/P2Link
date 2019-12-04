package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.core.stream.fragment.*;
import jokrey.utilities.network.link2peer.util.LongTupleList;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.simple.data_structure.pairs.Pair;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 *
 *
 * todo - this could(!) benefit from a 'x to 1' thread engine (because the batch send stuff needs to happen in intermediate steps..
 *        essentially a scheduler for tasks on a single thread (uses less threads and does the synchronization between tasks)
 *          enqueue tasks with a priority (one off tasks, continuous tasks), allow tasks to say whether they would like to be woken up
 *
 * @author jokrey
 */
public class P2LFragmentOutputStreamImplV1 extends P2LFragmentOutputStream {
    public static LossAcceptabilityCalculator lossConverter = new LossAcceptabilityCalculator_Packages(4);
    public static BatchSizeCalculatorCreator batchSizeCalculatorCreator = BatchSizeCalculator_StrikeDown.DEFAULT_CREATOR;

    public static final long DEFAULT_BATCH_DELAY = 333;
    public static final int RECEIPT_DELAY_MULTIPLIER = 3;

    private int packageSize;
    private long batch_delay_ms;

    private final BatchSizeCalculator batchSizeCalculator;
    protected P2LFragmentOutputStreamImplV1(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId, TransparentBytesStorage source) {
        super(parent, to, con, type, conversationId, source);
        int headerSize = new StreamPartHeader(null, type, conversationId, 0, false, false).getSize();
        packageSize = con==null?1024:con.remoteBufferSize - headerSize;
        batch_delay_ms = con==null? DEFAULT_BATCH_DELAY :Math.max(con.avRTT, DEFAULT_BATCH_DELAY);

        batchSizeCalculator = batchSizeCalculatorCreator.create(con);
    }


    //RUNTIME VARIABLES AND FLAGS
    private boolean allSend = false;
    private boolean allReceived = false;

    @Override public void sendSource(int timeout_ms) throws InterruptedException {
        double minTimeInSeconds = ((( source.contentSize() / (double)packageSize ) / (double)batchSizeCalculator.getBatchSize()) * batch_delay_ms) / 1000.0;
        System.out.println("minTime = " + minTimeInSeconds); //implementation can readily exceed min time - by adaptively increasing the batch_size

        long totalContent = source.contentSize();

        //todo - what if the peer is just straight up not available... when do we stop sending packages?

        long numSend = 0;
        while(numSend < totalContent)
            addLastInBatch(source.subStorage(numSend, Math.min(totalContent, numSend+=packageSize)));
        sendBatch();
        allSend=true;
        reEnqueueMissingRanges();

        while(!batch.isEmpty()) {
            sendBatch();
//            Thread.sleep(25);
        }

        SyncHelp.waitUntil(this, () -> allReceived, timeout_ms, () -> {
//            System.out.println("re");
            if(batch.isEmpty()) {
                if(latestMissingRanges == null || latestMissingRanges.isEmpty()) {
                    addLastInBatch(source.subStorage(Math.max(0, totalContent - packageSize), totalContent));
                } else
                    reEnqueueMissingRanges();
            }
            sendBatch();
        }, batch_delay_ms);
    }


    private LongTupleList latestMissingRanges = null;
    private long remoteLatestReceivedDataOffset = -1;
    private int receiptID = 0;

    ArrayList<Pair<Long, LongTupleList>> sendSinceLastReceipt = new ArrayList<>(RECEIPT_DELAY_MULTIPLIER+1);

    @Override public synchronized void receivedReceipt(P2LMessage rawReceipt) {
        P2LFragmentStreamReceipt receipt = P2LFragmentStreamReceipt.decode(rawReceipt);

        if(receipt.receiptID < receiptID && !(receiptID>0 && receipt.receiptID<0) )
            return;
        receiptID=receipt.receiptID;


        System.out.println("receivedReceipt - receipt.eof = " + receipt.eof+", receipt.latestReceived = " + receipt.latestReceived+", receipt.missingRanges = " + receipt.missingRanges);
        if(receipt.eof) {
            if(receipt.latestReceived >= source.contentSize() && receipt.missingRanges.isEmpty()) {
                allReceived = true;
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

            long totalLost = reEnqueueMissingRanges();

//            System.out.println("lossResult = " + lossResult);
//            System.out.println("batchSizeCalculator.getBatchSize() = " + batchSizeCalculator.getBatchSize());
//            System.out.println("batch_delay_ms = " + batch_delay_ms);
//            System.out.println("newlyKnownBytesDropped = " + recentlyLostBytes+"/"+totalLost);
//            System.out.println("packageSize = " + packageSize);
        }
        SyncHelp.notify(this);
    }

    private boolean isIn(long r1S, long r1E, long r2S, long r2E) {
        return r1S >= r2S && r1E <= r2E;
    }

    private synchronized long reEnqueueMissingRanges() {
        if(latestMissingRanges==null) return 0;

        LongTupleList latestMissing = new LongTupleList(latestMissingRanges.size()/2);
        long totalLostBytes = 0;
        for(int i = 0; i<latestMissingRanges.size(); i++) {
            long rangeS = latestMissingRanges.get0(i);
            long rangeE = latestMissingRanges.get1(i);
//            if(! allowResend(rangeS, rangeE, allSend)) {//A CERTAIN number of packages should be ignored, due to likely or possible reordering of packages by the receiver.. - does not happen often because RECEIPT_DELAY_MULTIPLIER
//                System.out.println("blocked from resend range = [" + rangeS+", "+rangeE+"]");
//                latestMissing.add(rangeS, rangeE);
//                continue;
//            }
            totalLostBytes+=rangeE-rangeS;

            try {
                enqueueRange(rangeS, rangeE);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
//        latestMissingRanges=null; //muy importante
        latestMissingRanges=latestMissing;

        return totalLostBytes;
    }

    private boolean allowResend(long rangeS, long rangeE, boolean allSend) {
//        return true; //in all honesty this feature seems to have little effect(due to the receipt delay multiplier)
        return allSend || highestOffsetSentInLastBatch<0 || rangeE < highestOffsetSentInLastBatch-((batchSizeCalculator.getBatchSize())*packageSize);
    }

    public static int numResend = 0;
    private void enqueueRange(long start, long end) throws IOException, InterruptedException {
        long curStart = start;
        while(curStart < end) {
            numResend++;
            System.out.println("resend("+packageSize+"): ["+curStart+", "+Math.min(end, curStart+packageSize)+"]");
            addFirstInBatch(source.subStorage(curStart, Math.min(end, curStart+=packageSize)));
            //todo - add feature that prohibits an explosion in buffer size..
        }
        sendBatch();
    }

    private ConcurrentLinkedDeque<SubBytesStorage> batch = new ConcurrentLinkedDeque<>();

    private void addFirstInBatch(SubBytesStorage toSend) {
        if(toSend.start == toSend.end()) return;
//        System.out.println("addFirstInBatch - toSend[" + toSend.start+", "+toSend.end()+"]");
        if(!batch.contains(toSend)) //todo maybe slower than it needs to
            batch.addFirst(toSend);
        if(batch.size() >= batchSizeCalculator.getBatchSize())
            sendBatch();
    }
    private void addLastInBatch(SubBytesStorage toSend) {
        if(toSend.start == toSend.end()) return;
//        System.out.println("addLastInBatch - toSend[" + toSend.start+", "+toSend.end()+"]");
        if(!batch.contains(toSend)) //todo maybe slower than it needs to
            batch.addLast(toSend);
        if(batch.size() >= batchSizeCalculator.getBatchSize())
            sendBatch();
    }

    private long lastBatchSentAt = -1;
    private long highestOffsetSentInLastBatch = -1;
    private int numPackagesSentInBatch = 0;
    private synchronized void sendBatch() {
        try {

//            System.out.println(System.currentTimeMillis()+" sendBatch("+batch.size()+")-("+numPackagesSentInBatch+"/"+batchSizeCalculator.getBatchSize()+"): elapsed = " + (System.currentTimeMillis() - lastBatchSentAt));

            endPackageIfPossible();

            filterBatchOfAlreadyReceivedPackages();

            SubBytesStorage packageContent;
            while(canSendPackagesInCurrentBatch() && (packageContent = batch.pollFirst()) != null) {
                P2LMessage message = buildP2LMessage(packageContent);
                numPackagesSentInBatch++;
                highestOffsetSentInLastBatch = Math.max(highestOffsetSentInLastBatch, packageContent.end());
                parent.sendInternalMessage(message, to);

                LongTupleList current = sendSinceLastReceipt.get(0).r;
                for (int i = current.size() - 1; i >= 0; i--) {
                    if(current.get1(i) == packageContent.start) {
                        current.set1(i, packageContent.end());
                        current=null;
                        break;
                    }
                }
                if(current != null)
                    current.add(packageContent.start, packageContent.end());
//                System.out.println("packageContent ["+packageContent.start+", "+packageContent.end()+"]");
                delaySendAppropriately();
                endPackageIfPossible();
            }

//            System.out.println(System.currentTimeMillis()+" sentBatch("+batch.size()+")("+numPackagesSentInBatch+"/"+batchSizeCalculator.getBatchSize()+"): elapsed = " + (System.currentTimeMillis() - lastBatchSentAt)+"\n");

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void filterBatchOfAlreadyReceivedPackages() {
        batch.removeIf(p -> {
            if(p.end >= remoteLatestReceivedDataOffset)
                return false;
            for(int i = 0; i<latestMissingRanges.size(); i++) {
                long rangeS = latestMissingRanges.get0(i);
                long rangeE = latestMissingRanges.get1(i);
                if(p.start >= rangeS && p.end <= rangeE)
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

    private P2LMessage buildP2LMessage(SubBytesStorage packageContent) {
        boolean lastPackage = packageContent.end() == source.contentSize();
        StreamPartHeader header = new StreamPartHeader(null, type, conversationId, (int) packageContent.start, false, lastPackage);
        byte[] content = header.generateRaw(packageContent.getContent());
        return new P2LMessage(header, null, content, (int) packageContent.contentSize());
    }

    private void delaySendAppropriately() throws InterruptedException {
        long elapsedInBatchWindow = System.currentTimeMillis() - lastBatchSentAt;
        long msLeftInCurrentBatchWindow = batch_delay_ms - elapsedInBatchWindow;
        long curPackageSupposedToBeSentAt = (long) ((batch_delay_ms / (double)batchSizeCalculator.getBatchSize()) * numPackagesSentInBatch);
        long msLeftToSendCurrentPackage = curPackageSupposedToBeSentAt - elapsedInBatchWindow; //send at the end of the package window.. - just like we are supposed to have send the batch batch-delay after the last batch was send..
//        System.out.println("sending " + System.currentTimeMillis()+": numSent("+numPackagesSentInBatch+"), curPackageSupposedToBeSentAt("+curPackageSupposedToBeSentAt+"), b_delay_ms("+batch_delay_ms+"), b_size("+batch_size+"), elapsedInWindow("+elapsedInBatchWindow+"), msLeftInCurrentBatchWindow("+msLeftInCurrentBatchWindow+"), msLeftToSendCurrentPackage("+msLeftToSendCurrentPackage+")");
        if(msLeftInCurrentBatchWindow > 0)
            if (msLeftToSendCurrentPackage > 1)//todo - this may be wrong, but a spin lock feels even more wrong....
                Thread.sleep(msLeftToSendCurrentPackage);
    }

    @Override public boolean waitForConfirmationOnAll(int timeout_ms) throws IOException {
        throw new UnsupportedOperationException(); //todo - though it is kinda made obsolete by sendSource...
    }
    @Override public boolean close(int timeout_ms) throws IOException {
        throw new UnsupportedOperationException(); //todo - though it is kinda made obsolete by sendSource...
    }
    @Override public boolean isClosed() {
        return allReceived || (remoteLatestReceivedDataOffset == -2);
    }

     public long[] calculateRecentlyLost(LongTupleList updatedMissingRanges) {
        long recentlyLostBytes = 0;
        long recentlySentBytes = 0;

        long now = System.currentTimeMillis();
        for (ListIterator<Pair<Long, LongTupleList>> iterator = sendSinceLastReceipt.listIterator(); iterator.hasNext(); ) {
            Pair<Long, LongTupleList> batchSent = iterator.next();
            for (int i = 0; i < batchSent.r.size(); i++) {
                long sentAt = (long) ((now - batchSent.l) + (batch_delay_ms * (i / (double) batchSent.r.size())));
                long thresholdToExpectReceival = now - (con==null?batch_delay_ms:con.avRTT*2);
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
