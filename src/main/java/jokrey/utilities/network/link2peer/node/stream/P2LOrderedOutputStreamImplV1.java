package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

/**
 * TODO - missing more advanced congestion control algorithm - current 'algorithm' works exclusively based on buffer size
 *     TODO - this entire stream algorithm should not be considered 'ready' - it is highly experimental and is missing critical features and optimizations
 *     TODO - it is little more than a demonstration of a
 *
 * TODO reuse header object - currently recreated with each part, could easily be reused
 * TODO    just reuse p2lMessage object
 *
 * TODO the three largest missing features: path mtu(maximum transmission unit size) discovery, congestion control and warp around after 2^31-1 packages
 *
 *
 * TODO TODO TODO  - investigate extreme drops in performance on package loss (requires timeout -> request receipt, which is insane)
 *     REASON: loss of last package (the package that requests the receipt)
 *
 * dynamically determined parameters:
 *    mtu
 *    average round time (request receipt -> received receipt)   (this is used to not resend packages to early)
 *    number of parallelly sendable packages (determined from number of requeried packages from max batch send[send within time x])
 *        (this is used for very fine tuned congestion control - though congestion control will also be )
 *
 * @author jokrey
 */
public class P2LOrderedOutputStreamImplV1 extends P2LOrderedOutputStream {
    private final ByteArrayStorage[] unconfirmedSendPackages = new ByteArrayStorage[P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE /2]; //this smaller is a simple, dumb, only mildly effective form of congestion control

    private int eofAtIndex = Integer.MAX_VALUE;
    private final byte[] unsendBuffer;
    private int unsendBufferFilledUpToIndex = 0;
    private boolean isUnsendBufferFull() {
        return unsendBufferFilledUpToIndex == unsendBuffer.length;
    }
    private int remainingBufferSpace() {
        return unsendBuffer.length - unsendBufferFilledUpToIndex;
    }
    private int headerSize;
    private int earliestUnconfirmedPartIndex = 0;
    private int latestAttemptedIndex = -1;

    public P2LOrderedOutputStreamImplV1(P2LNodeInternal parent, SocketAddress to, P2LConnection con, short type, short conversationId) {
        this(parent, to, (con==null?P2LMessage.CUSTOM_RAW_SIZE_LIMIT:con.remoteBufferSize), type, conversationId);
    }
    public P2LOrderedOutputStreamImplV1(P2LNodeInternal parent, SocketAddress to, int bufferSizeWithoutHeader, short type, short conversationId) {
        super(parent, to, null, type, conversationId);

        headerSize = new StreamPartHeader(null, toShort(type), toShort(conversationId), 0, false, false).getSize();
        unsendBuffer = new byte[bufferSizeWithoutHeader - headerSize];
    }

    @Override public synchronized void write(int b) throws IOException {
        if(eofAtIndex < latestAttemptedIndex+1) throw new IOException("Stream closed");
        unsendBuffer[unsendBufferFilledUpToIndex] = (byte) (b & 0xFF);
        unsendBufferFilledUpToIndex++;
        if(isUnsendBufferFull())
            packAndSend(false);
    }

    @Override public synchronized void write(byte[] b, int off, int len) throws IOException {
        if(eofAtIndex < latestAttemptedIndex+1) throw new IOException("Stream closed");
        int bytesSend = 0;
        while(bytesSend < len) {
            int remainingInB = len-bytesSend;
            int numBytesToPut = Math.min(remainingBufferSpace(), remainingInB);
            System.arraycopy(b, off, unsendBuffer, unsendBufferFilledUpToIndex, numBytesToPut);
            unsendBufferFilledUpToIndex+=numBytesToPut;
            bytesSend+=numBytesToPut;
            off+=numBytesToPut;
            if(isUnsendBufferFull())
                packAndSend(false);
        }
    }

    @Override public synchronized void flush() throws IOException {
        if(unsendBufferFilledUpToIndex!=0)
            packAndSend(false); //flush semantics currently means: pack and send, but does not include guarantees about receiving information
    }

    @Override public synchronized boolean close(int timeout_ms) throws IOException {
        if(eofAtIndex==Integer.MAX_VALUE) {//for reasons of impotence
            eofAtIndex = latestAttemptedIndex + 1;//i.e. next part
            packAndSend(true);//cannot use flush, flush checks if the buffer is empty, this is meant as a marker to the peer's input stream
            boolean confirmation = waitForConfirmationOnAll(timeout_ms);

            //help gc:
            Arrays.fill(unconfirmedSendPackages, null);
            Arrays.fill(shiftCache, null);

            parent.unregister(this);
            return confirmation;
        }
        return !hasUnconfirmedParts();
    }

    @Override public boolean isClosed() {
        return eofAtIndex < earliestUnconfirmedPartIndex && !hasUnconfirmedParts();
    }

    @Override public synchronized boolean waitForConfirmationOnAll(int timeout_ms) throws IOException {
        sendExtraordinaryReceiptRequest();

        return SyncHelp.waitUntilOrThrowIO(this, () -> !hasUnconfirmedParts(), timeout_ms, this::sendExtraordinaryReceiptRequest, P2LHeuristics.ORDERED_STREAM_V1_RECEIPT_TIMEOUT_MS);
    }
    private long lastReceiptRequest = 0;
    private boolean requestRecentlyMade() {
        return System.currentTimeMillis() - lastReceiptRequest < 500;
    }
    private synchronized void sendExtraordinaryReceiptRequest() throws IOException {
        if (!hasUnconfirmedParts() || requestRecentlyMade()) return;
        lastReceiptRequest = System.currentTimeMillis();
        if (isClosed()) {
            StreamPartHeader header = new StreamPartHeader(null, toShort(type), toShort(conversationId), -1, true, false);
            parent.sendInternalMessage(to, new P2LMessage(header, null, header.generateRaw(0), 0));
        } else{
            send(earliestUnconfirmedPartIndex, true);
        }
    }

    private boolean hasBufferCapacities() {
        return latestAttemptedIndex+1 < earliestUnconfirmedPartIndex+unconfirmedSendPackages.length;
    }
    private synchronized void waitForBufferCapacities(int timeout_ms) throws IOException {
        sendExtraordinaryReceiptRequest();
        SyncHelp.waitUntilOrThrowIO(this, this::hasBufferCapacities, timeout_ms, this::sendExtraordinaryReceiptRequest, P2LHeuristics.ORDERED_STREAM_V1_RECEIPT_TIMEOUT_MS);
    }

    private synchronized void packAndSend(boolean forceRequestReceipt) throws IOException {
        if(eofAtIndex < latestAttemptedIndex+1) throw new IOException("Stream closed");
        waitForBufferCapacities(0);

        //also send if buffer is empty - could be used as a marker (for example eof marker by close)
        int partIndexToSend = virtuallySend();
        send(partIndexToSend, forceRequestReceipt);

        unsendBufferFilledUpToIndex = 0;
    }
    private synchronized int virtuallySend() {
        latestAttemptedIndex++;

        int unconfirmedBufferIndex = latestAttemptedIndex - earliestUnconfirmedPartIndex;
        if(earliestUnconfirmedPartIndex > latestAttemptedIndex)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") < 0   [earliestUnconfirmedPartIndex= "+earliestUnconfirmedPartIndex+", latestAttemptedIndex= "+latestAttemptedIndex+"]");
        else if(unconfirmedBufferIndex >= unconfirmedSendPackages.length)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") >= unconfirmedSendPackages.length("+unconfirmedSendPackages.length+")");

        if(unconfirmedSendPackages[unconfirmedBufferIndex] == null)
            unconfirmedSendPackages[unconfirmedBufferIndex] = new ByteArrayStorage(headerSize + unsendBuffer.length);
        unconfirmedSendPackages[unconfirmedBufferIndex].size = headerSize;
        unconfirmedSendPackages[unconfirmedBufferIndex].set(headerSize, unsendBuffer, 0, unsendBufferFilledUpToIndex);
        return latestAttemptedIndex;
    }
    private synchronized void send(int partIndexToSend, boolean forceRequestReceipt) throws IOException {
        int bufferIndexToSend = partIndexToSend - earliestUnconfirmedPartIndex;
        if(bufferIndexToSend < 0) throw new IllegalStateException("buffer index("+bufferIndexToSend+") negative - this should not happen");
        if(bufferIndexToSend >= unconfirmedSendPackages.length) throw new IllegalStateException("buffer index("+bufferIndexToSend+") positive out of bounds("+unconfirmedSendPackages.length+") - this should not happen");
        ByteArrayStorage chunk = unconfirmedSendPackages[bufferIndexToSend];

        boolean eof = eofAtIndex == partIndexToSend;
        boolean thisPartIsLastInUnconfirmedBuffer = partIndexToSend + 1 >= earliestUnconfirmedPartIndex + unconfirmedSendPackages.length;
        boolean requestReceipt = forceRequestReceipt || eof || thisPartIsLastInUnconfirmedBuffer; //eof always requests a receipt, because the close method will not
        StreamPartHeader header = new StreamPartHeader(null, toShort(type), toShort(conversationId), partIndexToSend, requestReceipt, eof);
        if(requestReceipt)
            lastReceiptRequest=System.currentTimeMillis();
        header.writeTo(chunk.content); //if request receipt was changed...
        parent.sendInternalMessage(to, new P2LMessage(header, null, chunk.content, (int) (chunk.contentSize() - headerSize)));
    }


    @Override public void receivedReceipt(P2LMessage rawReceipt) {
        P2LOrderedStreamReceipt missingParts = P2LOrderedStreamReceipt.decode(rawReceipt);
        handleReceipt(missingParts);//blocking
    }

    private final ByteArrayStorage[] shiftCache = new ByteArrayStorage[unconfirmedSendPackages.length/2];
    private synchronized void handleReceipt(P2LOrderedStreamReceipt receipt) {
        int latestIndexReceivedByPeer = receipt.latestReceived;
        int[] missingParts = receipt.missingParts;
        if(receipt.eof) {
            eofAtIndex = latestIndexReceivedByPeer;//i.e. now
//            parent.removeStreamReceiptListener(to, type, conversationId);
//            return;
        }

        System.out.println("handleReceipt - missingParts = " + Arrays.toString(missingParts));

        if(latestIndexReceivedByPeer+1 < earliestUnconfirmedPartIndex)
            return;//received old receipt

        int newEarliestUnconfirmedIndex;
        if(missingParts.length == 0) {
            newEarliestUnconfirmedIndex = latestIndexReceivedByPeer+1;
        } else {
            Arrays.sort(missingParts);
            newEarliestUnconfirmedIndex = missingParts[0];

            if(newEarliestUnconfirmedIndex <= latestAttemptedIndex) {
                try {
                    if (newEarliestUnconfirmedIndex < earliestUnconfirmedPartIndex)
                        return; //then this receipt is not current anymore

                    int lastMissingPart = newEarliestUnconfirmedIndex;
                    for (int missingPart : missingParts) {
                        if (newEarliestUnconfirmedIndex == -1)
                            newEarliestUnconfirmedIndex = missingPart;

                        markConfirmed(lastMissingPart+1, missingPart-1);

                        if (isConfirmed(missingPart)) {
                            if (missingPart == newEarliestUnconfirmedIndex)
                                newEarliestUnconfirmedIndex = -1;
                        } else {
                            System.out.println("handleReceipt - resend missingPart = " + missingPart);
                            DebugStats.orderedStream1_numResend.getAndIncrement();
                            send(missingPart, !requestRecentlyMade() && eofAtIndex!=-1);
                        }
                        lastMissingPart = missingPart;
                    }
                    if (newEarliestUnconfirmedIndex == -1)
                        newEarliestUnconfirmedIndex = latestIndexReceivedByPeer + 1;
                } catch (IOException e) {
                    e.printStackTrace();
                    //todo mark broken, etc...
                }
            }
//            else newEarliestUnconfirmedIndex > latestAttemptedIndex : the peer notifies us that it has been waiting on a part we have not send yet - it might do this in case of delayed receipt requests
        }
        int shiftBy = newEarliestUnconfirmedIndex - earliestUnconfirmedPartIndex;
        markConfirmed(earliestUnconfirmedPartIndex,
                earliestUnconfirmedPartIndex+ (shiftBy-1));
        //ARRAY COPY DOES NOT SUFFICE - consider: [p1, p2, p3] -shift-> [p2, p3, p3] - when p3 at i=2 is marked as completed, p3 at position i=1 would be as well...
//        System.arraycopy(unconfirmedSendPackages, shiftBy, unconfirmedSendPackages, 0, unconfirmedSendPackages.length - shiftBy);
        BitHelper.rotateLeftBy(unconfirmedSendPackages, unconfirmedSendPackages.length, shiftCache, shiftBy);
        earliestUnconfirmedPartIndex = newEarliestUnconfirmedIndex;

        notify();
    }

    private synchronized boolean hasUnconfirmedParts() {
        if(earliestUnconfirmedPartIndex > latestAttemptedIndex+1)
            throw new IllegalStateException("earliest unconfirmed part index is greater than the next package to be send - i.e. we have confirmation for a package we did not send");
        return earliestUnconfirmedPartIndex != latestAttemptedIndex+1;//i.e. the earliest unconfirmed is the part we have not send yet
    }

    private synchronized boolean isConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        return unconfirmedSendPackages[bufferIndex] == null || unconfirmedSendPackages[bufferIndex].size==-1;//todo size shou
    }
    private synchronized void markConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        if(unconfirmedSendPackages[bufferIndex] != null)
            unconfirmedSendPackages[bufferIndex].size=-1;
    }
    private void markConfirmed(int firstPartIndex, int lastPartIndex) {
        for (int partI = firstPartIndex; partI <= lastPartIndex; partI++)
            markConfirmed(partI);
    }
}