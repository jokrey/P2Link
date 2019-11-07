package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.util.DataChunk;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;

/**
 * TODO - missing more advanced congestion control algorithm - current 'algorithm' works soley based on buffer size
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
 *
 * @author jokrey
 */
public class P2LOutputStreamV1 extends P2LOutputStream {
    private final DataChunk[] unconfirmedSendPackages = new DataChunk[P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE]; //technically + 1, because the receiving input stream does not store the first unreceived package

    private int eofAtIndex = Integer.MAX_VALUE;
    private final DataChunk unsendBuffer;
    private int headerSize;
    private int earliestUnconfirmedPartIndex = 0;
    private int latestAttemptedIndex = -1;

    public P2LOutputStreamV1(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        super(parent, to, type, conversationId);

        byte[] rawBuffer = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT];
        headerSize = new StreamPartHeader(null, type, conversationId, 0, false, false).getSize();
        unsendBuffer = new DataChunk(rawBuffer, headerSize, 0);

        boolean registeringReceiptListenerSuccessful = parent.setStreamReceiptListener(to, type, conversationId, message -> {
            StreamReceipt missingParts = StreamReceipt.decode(message);
            handleReceipt(missingParts);//blocking
        });
        if(!registeringReceiptListenerSuccessful)
            throw new IllegalStateException("Stream occupied - another stream is already listening for the specific type-conversationId combination");
    }

    @Override public synchronized void write(int b) throws IOException {
        if(eofAtIndex < latestAttemptedIndex+1) throw new IOException("Stream closed");
        unsendBuffer.put((byte)(b & 0xFF));
        if(unsendBuffer.isFull())
            packAndSend(false);
    }

    @Override public synchronized void write(byte[] b, int off, int len) throws IOException {
        if(eofAtIndex < latestAttemptedIndex+1) throw new IOException("Stream closed");
        int bytesSend = 0;
        while(bytesSend < len) {
            int remainingInB = len-bytesSend;
            int numBytesToPut = Math.min(unsendBuffer.remainingSpace(), remainingInB);
            unsendBuffer.put(b, off, numBytesToPut);
            bytesSend+=numBytesToPut;
            off+=numBytesToPut;
            if(unsendBuffer.isFull())
                packAndSend(false);
        }
    }

    @Override public synchronized void flush() throws IOException {
        packAndSend(false); //flush semantics currently means: pack and send, but does not include guarantees about receiving information
    }

    @Override public synchronized boolean close(int timeout_ms) throws IOException {
        if(eofAtIndex==Integer.MAX_VALUE) {//for reasons of impotence
            eofAtIndex = latestAttemptedIndex + 1;//i.e. next part
            packAndSend(true);
            boolean confirmation = waitForConfirmationOnAll(timeout_ms);
            parent.removeStreamReceiptListener(to, type, conversationId);

            //to help gc: (does this help gc? - array list default implementation does it, kinda)
            for(int i=0;i<unconfirmedSendPackages.length;i++)
                unconfirmedSendPackages[i] = null;
            for(int i=0;i<shiftCache.length;i++)
                shiftCache[i] = null;
            return confirmation;
        }
        return !hasUnconfirmedParts();
    }

    @Override public boolean isClosed() {
        return eofAtIndex < earliestUnconfirmedPartIndex && !hasUnconfirmedParts();
    }

    @Override public synchronized boolean waitForConfirmationOnAll(int timeout_ms) throws IOException {
        try {
            sendExtraordinaryReceiptRequest();

            long startCtm = System.currentTimeMillis();
            long elapsed = 0;
            while(hasUnconfirmedParts()) {
//                System.out.println("P2LOutputStreamV1.waitForConfirmationOnAll - earliestUnconfirmedPartIndex("+earliestUnconfirmedPartIndex+"), latestAttemptedIndex("+latestAttemptedIndex+")");
                wait(timeout_ms ==0? P2LHeuristics.STREAM_RECEIPT_TIMEOUT_MS:Math.min(timeout_ms-elapsed, P2LHeuristics.STREAM_RECEIPT_TIMEOUT_MS));
                elapsed = System.currentTimeMillis() - startCtm;
                if(timeout_ms != 0 && elapsed >= timeout_ms) return false;

                if(hasUnconfirmedParts())
                    sendExtraordinaryReceiptRequest();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return true;
    }
    private long lastReceiptRequest = 0;
    private boolean requestRecentlyMade() {
        return System.currentTimeMillis() - lastReceiptRequest < 500;
    }
    private synchronized void sendExtraordinaryReceiptRequest() throws IOException {
        if(!hasUnconfirmedParts() || requestRecentlyMade()) return;
        lastReceiptRequest=System.currentTimeMillis();
        try {
            StreamPartHeader header = new StreamPartHeader(null, type, conversationId, -1, true, false);
            parent.sendInternalMessage(new P2LMessage(header, null, header.generateRaw(0), 0, null), to);
        } catch (IOException e) {
            //todo mark broken, etc...
            throw e;
        }
    }

    public boolean hasBufferCapacities() {
        return latestAttemptedIndex+1 < earliestUnconfirmedPartIndex+unconfirmedSendPackages.length;
    }
    public synchronized void waitForBufferCapacities(int timeout_ms) throws IOException {
        try {
            sendExtraordinaryReceiptRequest();
            long startCtm = System.currentTimeMillis();
            long elapsed = 0;
            while(!hasBufferCapacities()) {
                wait(timeout_ms ==0? P2LHeuristics.STREAM_RECEIPT_TIMEOUT_MS:Math.min(timeout_ms-elapsed, P2LHeuristics.STREAM_RECEIPT_TIMEOUT_MS));
                elapsed = System.currentTimeMillis() - startCtm;
                if(timeout_ms != 0 && elapsed >= timeout_ms) throw new IOException("waiting for confirmation timed out after "+elapsed+"ms");

                if(!hasBufferCapacities())
                    sendExtraordinaryReceiptRequest();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private synchronized void packAndSend(boolean forceRequestReceipt) throws IOException {
        if(eofAtIndex < latestAttemptedIndex+1) throw new IOException("Stream closed");
        waitForBufferCapacities(0);
        unsendBuffer.offset = headerSize; //because offset is used to mark 'em as confirmed
        if(!unsendBuffer.isEmpty()) {
            int partIndexToSend = virtuallySend();
            send(partIndexToSend, forceRequestReceipt);

            unsendBuffer.offset = unsendBuffer.lastDataIndex = headerSize;//clear to offset
        }
    }
    private synchronized int virtuallySend() {
        latestAttemptedIndex++;

        int unconfirmedBufferIndex = latestAttemptedIndex - earliestUnconfirmedPartIndex;
        if(earliestUnconfirmedPartIndex > latestAttemptedIndex)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") < 0   [earliestUnconfirmedPartIndex= "+earliestUnconfirmedPartIndex+", latestAttemptedIndex= "+latestAttemptedIndex+"]");
        else if(unconfirmedBufferIndex >= unconfirmedSendPackages.length)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") >= unconfirmedSendPackages.length("+unconfirmedSendPackages.length+")");

        if(unconfirmedSendPackages[unconfirmedBufferIndex] == null || unsendBuffer.lastDataIndex > unconfirmedSendPackages[unconfirmedBufferIndex].capacity())
            unconfirmedSendPackages[unconfirmedBufferIndex] = new DataChunk(unsendBuffer.capacity());
        unsendBuffer.cloneInto(unconfirmedSendPackages[unconfirmedBufferIndex]);
        return latestAttemptedIndex;
    }
    private synchronized void send(int partIndexToSend, boolean forceRequestReceipt) throws IOException {
        int bufferIndexToSend = partIndexToSend - earliestUnconfirmedPartIndex;
        if(bufferIndexToSend < 0) throw new IllegalStateException("buffer index("+bufferIndexToSend+") negative - this should not happen");
        if(bufferIndexToSend >= unconfirmedSendPackages.length) throw new IllegalStateException("buffer index("+bufferIndexToSend+") positive out of bounds("+unconfirmedSendPackages.length+") - this should not happen");
        DataChunk chunk = unconfirmedSendPackages[bufferIndexToSend];

        boolean eof = eofAtIndex == partIndexToSend;
        boolean thisPartIsLastInUnconfirmedBuffer = partIndexToSend + 1 >= earliestUnconfirmedPartIndex + unconfirmedSendPackages.length;
        boolean requestReceipt = forceRequestReceipt || eof || thisPartIsLastInUnconfirmedBuffer; //eof always requests a receipt, because the close method will not
        StreamPartHeader header = new StreamPartHeader(null, type, conversationId, partIndexToSend, requestReceipt, eof);
        if(requestReceipt)
            lastReceiptRequest=System.currentTimeMillis();
        header.writeTo(chunk.data);
        parent.sendInternalMessage(new P2LMessage(header, null, chunk.data, chunk.lastDataIndex - headerSize, null), to);
    }



    private final DataChunk[] shiftCache = new DataChunk[unconfirmedSendPackages.length/2];
    private synchronized void handleReceipt(StreamReceipt receipt) {
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
        return unconfirmedSendPackages[bufferIndex] == null || unconfirmedSendPackages[bufferIndex].offset==-1;
    }
    private synchronized void markConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        if(unconfirmedSendPackages[bufferIndex] != null)
            unconfirmedSendPackages[bufferIndex].offset=-1;
    }
    private void markConfirmed(int firstPartIndex, int lastPartIndex) {
        for (int partI = firstPartIndex; partI <= lastPartIndex; partI++)
            markConfirmed(partI);
    }
}