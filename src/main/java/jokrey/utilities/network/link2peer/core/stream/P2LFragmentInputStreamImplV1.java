package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.message_headers.StreamReceiptHeader;
import jokrey.utilities.network.link2peer.util.LongTupleList;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.stream.P2LFragmentOutputStreamImplV1.DEFAULT_BATCH_DELAY;
import static jokrey.utilities.network.link2peer.core.stream.P2LFragmentOutputStreamImplV1.RECEIPT_DELAY_MULTIPLIER;

/**
 * @author jokrey
 */
public class P2LFragmentInputStreamImplV1 extends P2LFragmentInputStream {
    public static int doubleReceived = 0;
    public static int validReceived = 0;

    private int receiptPackageMaxSize;
    long receipt_delay_ms;
    protected P2LFragmentInputStreamImplV1(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId, TransparentBytesStorage target) {
        super(parent, to, con, type, conversationId);
        int headerSize = new StreamReceiptHeader(null, type, conversationId,false).getSize();
        receiptPackageMaxSize = con==null?1024:con.remoteBufferSize - headerSize;
        long batch_delay_ms = con==null? DEFAULT_BATCH_DELAY :Math.max(con.avRTT, DEFAULT_BATCH_DELAY);
        receipt_delay_ms = batch_delay_ms * RECEIPT_DELAY_MULTIPLIER;//con==null?50:con.avRTT/2;
    }

    private LongTupleList missingRanges = new LongTupleList();
    private long highestEndReceived = 0;
    private long eofAt = -1;


    long lastReceiptSendAt = System.currentTimeMillis();
    int numPackagesReceivedInLastBatch = 0;

    //todo - synchronization might be bad, and not strictly required
    //    however there is a race condition where fireReceived does not complete, but the output stream already receives a receipt that it has completed - that might not matter in reality, but can screw up a check

    private int receiptID = 0;
    @Override public synchronized void received(P2LMessage message) {
        try {
            if(forceClosedByThis) {
                sendReceipt(1024);
                return;
            }

            long startOffset = message.header.getPartIndex();
            long endOffset = startOffset + message.getPayloadLength();

            boolean wasMissing = markReceived(startOffset, endOffset);
            if (wasMissing) {
                fireReceived(message.header.getPartIndex(), message.content, message.header.getSize(), message.getPayloadLength());
                validReceived++;
            } else {
                doubleReceived++;
            }
    //        System.err.println("wasMissing = " + wasMissing);

            if(message.header.isStreamEof())
                eofAt = endOffset;
            if(isFullyReceived())
                SyncHelp.notify(this);

            numPackagesReceivedInLastBatch++;

            long elapsedSinceLastReceipt = System.currentTimeMillis() - lastReceiptSendAt;
    //        System.out.println("isFullyReceived() = " + isFullyReceived());
    //        System.out.println("received = [" + startOffset+", "+endOffset+"]");
    //        System.out.println("missingRanges = " + missingRanges);
            if(elapsedSinceLastReceipt > receipt_delay_ms ||
                ((isFullyReceived() || message.header.isStreamEof()) && wasMissing) ||
                message.header.requestReceipt()) {
                System.out.println("SEND RECEIPT("+receiptPackageMaxSize+", "+P2LMessage.CUSTOM_RAW_SIZE_LIMIT+") receipt.latestReceived = " + highestEndReceived+", receipt.missingRanges("+missingRanges.size()+") = " + missingRanges);
                sendReceipt(receiptPackageMaxSize);
                lastReceiptSendAt = System.currentTimeMillis();
                numPackagesReceivedInLastBatch=0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override public boolean isFullyReceived() {
        return eofAt != -1 && highestEndReceived == eofAt && missingRanges.isEmpty();
    }
    @Override public boolean waitForFullyReceived(int timeout_ms) {
        try {
            return SyncHelp.waitUntil(this, this::isFullyReceived, timeout_ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    private boolean forceClosedByThis = false;
    @Override public void close() throws IOException {
//        waitForAllReceived(0);
        forceClosedByThis =true;
        sendReceipt(1024);//curtsy
    }

    private void sendReceipt(int receiptPackageMaxSize) throws IOException {
        parent.sendInternalMessage(P2LFragmentStreamReceipt.encode(type, conversationId, isClosed(), (int) highestEndReceived, missingRanges, receiptID++, receiptPackageMaxSize), to);
    }

    @Override public boolean isClosed() {
        return forceClosedByThis || isFullyReceived();
    }



    private synchronized boolean markReceived(long start, long end) {
//        System.err.println("markReceived - start = " + start + ", " + end);
//        System.err.println("missingRanges before = " + missingRanges);
//        try {
        if (start >= highestEndReceived) {
            if (start > highestEndReceived)
                missingRanges.add(highestEndReceived, start);
            highestEndReceived = end;
            return true;
        } else if (end >= highestEndReceived) {
            highestEndReceived = end;
            return true;
        } else {
            for (int i=0; i < missingRanges.size(); i++) {
                long rangeStart = missingRanges.get0(i);
                long rangeEnd = missingRanges.get1(i);
                if (start >= rangeStart && end <= rangeEnd) {
                    if(start == rangeStart && end == rangeEnd)
                        missingRanges.fastRemoveIndex(i);
                    else if (start > rangeStart && end < rangeEnd) {//the following is done to preserve order, to make the algorithm in the else-if simple
                        missingRanges.set(i, end, rangeEnd);
                        missingRanges.add(i, rangeStart, start);
                    } else if (start > rangeStart)
                        missingRanges.set(i, rangeStart, start);
                    else if (end < rangeEnd)
                        missingRanges.set(i, end, rangeEnd);
                    return true;
                } else if (start >= rangeStart && start < rangeEnd) {
                    //start in, but end out of range

                    int removeIndexOfStartTuple = -1;
                    if (start > rangeStart)
                        missingRanges.set(i, rangeStart, start);
                    else
                        removeIndexOfStartTuple=i;

                    i++;
                    for (; i < missingRanges.size();i++) {
                        long innerRangeStart = missingRanges.get0(i);
                        long innerRangeEnd = missingRanges.get1(i);

                        if (end >= innerRangeStart && end <= innerRangeEnd) {
                            //end in, but start out of range
                            if (end < innerRangeEnd) {
                                if(removeIndexOfStartTuple==-1)
                                    missingRanges.add(i, end, innerRangeEnd);
                                else {
                                    removeIndexOfStartTuple=-1;
                                    missingRanges.set(i, rangeStart, start);
                                }
                            } else
                                missingRanges.fastRemoveIndex(i);
                            break;
                        } else {
                            missingRanges.fastRemoveIndex(i); //remove all intermediate ranges (because they are between start and end)
                        }
                    }

                    if(removeIndexOfStartTuple!=-1)
                        missingRanges.fastRemoveIndex(removeIndexOfStartTuple);

                    return true;
                }
            }
            return false;
        }
//        } finally {
//            System.err.println("missingRanges after = " + missingRanges);
//            System.err.println("highestEndReceived: "+highestEndReceived);
//            long lastEnd = 0;
//            for (int i=0; i < missingRanges.size(); i++) {
//                long rangeStart = missingRanges.get0(i);
//                long rangeEnd = missingRanges.get1(i);
//                if(rangeStart < lastEnd || rangeEnd < lastEnd)
//                    System.err.println("FUCK rangeStart("+rangeStart+") < lastEnd("+lastEnd+") || rangeEnd("+rangeEnd+") < lastEnd("+lastEnd+")");
//                lastEnd = rangeEnd;
//            }
//        }
    }
}
