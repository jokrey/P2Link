package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.StreamReceiptHeader;
import jokrey.utilities.network.link2peer.util.LongTupleList;
import jokrey.utilities.network.link2peer.util.SyncHelp;

import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.node.stream.P2LFragmentOutputStreamImplV1.DEFAULT_BATCH_DELAY;
import static jokrey.utilities.network.link2peer.node.stream.P2LFragmentOutputStreamImplV1.RECEIPT_DELAY_MULTIPLIER;

/**
 * Why is a time based receipt better?:
 *   We don't want a receipt for EVERY package, only for a batch
 *   We could request a receipt with the last package, but what if it is dropped
 *   We could send a receipt after n packages, but we don't know the current batch size
 *
 * @author jokrey
 */
public class P2LFragmentInputStreamImplV1 extends P2LFragmentInputStream {
    private int receiptPackageMaxSize;
    long receipt_delay_ms;
    protected P2LFragmentInputStreamImplV1(P2LNodeInternal parent, SocketAddress to, P2LConnection con, short type, short conversationId, short step) {
        super(parent, to, con, type, conversationId, step);
        int headerSize = new StreamReceiptHeader(null, type, conversationId, step,false).getSize();
        receiptPackageMaxSize = con==null?1024:con.remoteBufferSize - headerSize;
        long batch_delay_ms = con==null? DEFAULT_BATCH_DELAY :Math.max(con.avRTT, DEFAULT_BATCH_DELAY);
        receipt_delay_ms = batch_delay_ms * RECEIPT_DELAY_MULTIPLIER;//con==null?50:con.avRTT/2;
    }

    private LongTupleList missingRanges = new LongTupleList();
    private long highestEndReceived = 0;
    private long eofAt = -1;

    public long getEarliestMissingIndex() {
        return missingRanges==null||missingRanges.isEmpty()?highestEndReceived : missingRanges.get0(0);
    }


    long lastReceiptSendAt = System.currentTimeMillis();
    int numPackagesReceivedInLastBatch = 0;

    private int receiptID = 0;
    //todo - synchronization might be bad, and not strictly required
    //    however there is a race condition where fireReceived does not complete, but the output stream already receives a receipt that it has completed - that might not matter in reality, but can screw up a check
    //    this does entail that if fireReceived takes a long time to complete(like 10-100ms), the entire performance will drop very significantly, since many packages can only be handled late since they all wait at the sync barrier (and the algorithm is largely based on time)
    @Override public synchronized void received(P2LMessage message) {
        try {
            if(forceClosedByThis) {
                sendReceipt(1024);
                return;
            }

            boolean thisMessageWasTheOneThatToldUsItWasFinallyOver = message.header.isStreamEof() && eofAt==-1;

            long startOffset = message.header.getPartIndex();
            long endOffset = startOffset + message.getPayloadLength();

            boolean wasMissing = markReceived(startOffset, endOffset);
            if (wasMissing) {
                if(thisMessageWasTheOneThatToldUsItWasFinallyOver) {
                    fireReceived(message.header.getPartIndex(), message.content, message.header.getSize(), message.getPayloadLength(), true);
                    eofAt = endOffset;
                } else
                    fireReceived(message.header.getPartIndex(), message.content, message.header.getSize(), message.getPayloadLength(), false);
                DebugStats.fragmentStream1_validReceived.getAndIncrement();
            } else {
                DebugStats.fragmentStream1_doubleReceived.getAndIncrement();
            }
//            System.err.println("wasMissing = " + wasMissing);

            if(thisMessageWasTheOneThatToldUsItWasFinallyOver) {
                eofAt = endOffset;
                fireReceived(endOffset, new byte[0], 0, 0, true);
            }
//            System.err.println("eof = " + eofAt);
            if(isFullyReceived())
                SyncHelp.notify(this);

            numPackagesReceivedInLastBatch++;

            long elapsedSinceLastReceipt = System.currentTimeMillis() - lastReceiptSendAt;
//            System.out.println("isFullyReceived() = " + isFullyReceived());
//            System.out.println("received = [" + startOffset+", "+endOffset+"]");
            if(elapsedSinceLastReceipt > receipt_delay_ms ||
                ((isFullyReceived() || message.header.isStreamEof()) && wasMissing) ||
                message.header.requestReceipt() ||
                thisMessageWasTheOneThatToldUsItWasFinallyOver) {
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
        return SyncHelp.waitUntil(this, this::isFullyReceived, timeout_ms);
    }

    private boolean forceClosedByThis = false;
    @Override public void close() throws IOException {
        if(!isClosed()) {
            forceClosedByThis=true;
            sendReceipt(1024);//curtsy
        } else
            forceClosedByThis=true;
        parent.unregister(this);
    }

    private synchronized void sendReceipt(int receiptPackageMaxSize) throws IOException {
        System.out.println("SEND RECEIPT("+receiptPackageMaxSize+", "+P2LMessage.CUSTOM_RAW_SIZE_LIMIT+") receipt.latestReceived = " + highestEndReceived+", receipt.missingRanges("+missingRanges.size()+") = " + missingRanges);
        parent.sendInternalMessage(from, P2LFragmentStreamReceipt.encode(type, conversationId, step, isClosed(), (int) highestEndReceived, missingRanges, receiptID++, receiptPackageMaxSize));
    }

    @Override public boolean isClosed() {
        return forceClosedByThis || isFullyReceived();
    }



    private synchronized boolean markReceived(long start, long end) {
//        System.err.println("B - missingRanges(highRecv:"+highestEndReceived+") before("+start + ", " + end+") = " + missingRanges);
//        try {
            long newHighestEndReceived = markReceived(highestEndReceived, missingRanges, start, end);
            if(newHighestEndReceived == -1)
                return false;
            highestEndReceived = newHighestEndReceived;
            return true;
//        } finally {
//            System.err.println("A - missingRanges(highRecv:"+highestEndReceived+") after("+start + ", " + end+") = " + missingRanges);
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





    public static long markReceived(long highestEndReceived, LongTupleList missingRanges, long start, long end) {
        if(start == end) {
            System.err.println("FUCK! start == end == "+start);
            return -1;
        }
        if (start >= highestEndReceived) {
            if (start > highestEndReceived)
                missingRanges.add(highestEndReceived, start);
            return end;
//        } else if (end >= highestEndReceived) {
//            return end;
        } else {
            boolean anyMissingFound = false;
            for (int i=0; i < missingRanges.size(); i++) {
                long rangeStart = missingRanges.get0(i);
                long rangeEnd = missingRanges.get1(i);
                if(rangeStart < start && rangeEnd < start)
                    break;//because missing ranges have a guaranteed order...
                else if((rangeStart > end && rangeEnd > end))
                    continue; //ranges do not touch
                else {
                    if (start == rangeStart && end == rangeEnd) {
                        //received is exactly the missing range...
                        missingRanges.fastRemoveIndex(i);
                        return highestEndReceived; //no further ranges can possibly touch the received range
                    } else if (start >= rangeStart && end <= rangeEnd) {
                        //received is entirely contained
                        if(end != rangeEnd && rangeStart != start) {
                            missingRanges.set(i, end, rangeEnd); //done to preserve order
                            missingRanges.add(i, rangeStart, start); //done to preserve order
                        } else if(end != rangeEnd)
                            missingRanges.set(i, end, rangeEnd); //done to preserve order
                        else if (rangeStart != start)
                            missingRanges.set(i, rangeStart, start); //done to preserve order
                        return highestEndReceived; //no further ranges can possibly touch the received range
                    } else if (start >= rangeStart && start < rangeEnd && end > rangeEnd) {
                        //missing range contains start, but not end
                        if (start == rangeStart)
                            missingRanges.fastRemoveIndex(i --);
                        else if (start > rangeStart)
                            missingRanges.set(i, rangeStart, start);
                        anyMissingFound=true;
                    } else if (start < rangeStart && end > rangeEnd) {
                        //entire missing range is contained
                        missingRanges.fastRemoveIndex(i --);
                        anyMissingFound=true;
                    } else if (start <= rangeStart && end > rangeStart) {
                        //newly received contains range start, but not range end
                        missingRanges.set(i, end, rangeEnd);
                        anyMissingFound=true;
                    } else {
                        throw new UnsupportedOperationException("missing handling for: missing:{"+rangeStart+", "+rangeEnd+"} - new:{"+start+", "+end+"}");
                    }
                }
            }

            if(end > highestEndReceived) return end;
            else if(anyMissingFound) return highestEndReceived;
            else return -1;
        }
    }
}
