package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.message_headers.StreamPartHeader;
import jokrey.utilities.network.link2peer.util.DataChunk;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.util.Arrays;

/**
 * TODO - missing more advanced congestion control algorithm - current 'algorithm' works soley based on buffer size
 *     TODO - this entire stream algorithm should not be considered 'ready' - it is highly experimental and is missing critical features and optimizations
 *
 * TODO _ REUSE BUFFERS (currently they are just set to null, but they could easily be reused)
 * TODO reuse header object - currently recreated with each part, could easily be reused
 * TODO    just reuse package object
 *
 * @author jokrey
 */
public class P2LOutputStream extends OutputStream implements AutoCloseable {
    private final P2LNodeInternal parent;
    private final SocketAddress to;
    private final int type;
    private final int conversationId;

    private final DataChunk[] unconfirmedSendPackages = new DataChunk[P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE]; //technically + 1, because the receiving input stream does not store the first unreceived package
//    private int remainingUnconfirmedBufferCapacity = unconfirmedSendPackages.length-1;
    private synchronized boolean isConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        return unconfirmedSendPackages[bufferIndex] == null || unconfirmedSendPackages[bufferIndex].offset==-1;
    }
    private synchronized void markConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        if(unconfirmedSendPackages[bufferIndex] != null)
            //todo - marking confirmed via offset causes a weird error in handle receipt - things are considered confirmed which are definitely not confirmed
//            unconfirmedSendPackages[bufferIndex].offset=-1;
            unconfirmedSendPackages[bufferIndex]=null;
    }

    private int eofAtIndex = -1;
    private final DataChunk unsendBuffer;
    private int headerSize;
    private int earliestUnconfirmedPartIndex = 0;
    private int latestAttemptedIndex = -1;
    private synchronized boolean hasUnconfirmedParts() {
        if(earliestUnconfirmedPartIndex > latestAttemptedIndex+1)
            throw new IllegalStateException("earliest unconfirmed part index is greater than the next package to be send - i.e. we have confirmation for a package we did not send");
        return earliestUnconfirmedPartIndex != latestAttemptedIndex+1;//i.e. the earliest unconfirmed is the part we have not send yet
    }

    public P2LOutputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.type = type;
        this.conversationId = conversationId;

        byte[] rawBuffer = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT];
        headerSize = new StreamPartHeader(null, type, conversationId, 0, false, false).getSize();
        unsendBuffer = new DataChunk(rawBuffer, headerSize, 0);

        parent.setStreamReceiptListener(to, type, conversationId, message -> {
            Pair<Integer, int[]> missingParts = StreamReceipt.decode(message);
            handleReceipt(missingParts);//blocking
        });
    }

    @Override public synchronized void write(int b) throws IOException {
        unsendBuffer.put((byte)(b & 0xFF));
        if(unsendBuffer.isFull())
            packAndSend(false);
    }

    @Override public synchronized void write(byte[] b, int off, int len) throws IOException {
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

    @Override public synchronized void close() throws IOException {
        if(eofAtIndex==-1) {//for reasons of impotence
            eofAtIndex = latestAttemptedIndex + 1;//i.e. next part
            packAndSend(true);
            waitForConfirmationOnAll();
        }
    }


    private synchronized void waitForConfirmationOnAll() throws IOException {
        try {
            while(hasUnconfirmedParts()) {
                sendExtraProcessReceiptRequest();
//                System.out.println("P2LOutputStream.waitForConfirmationOnAll - earliestUnconfirmedPartIndex("+earliestUnconfirmedPartIndex+"), latestAttemptedIndex("+latestAttemptedIndex+")");
                wait(P2LHeuristics.STREAM_RECEIPT_TIMEOUT_MS);
                sendExtraProcessReceiptRequest();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    private long lastReceiptRequest = 0;
    private boolean requestRecentlyMade() {
        return System.currentTimeMillis() - lastReceiptRequest < 500;
    }
    private synchronized void sendExtraProcessReceiptRequest() throws IOException {
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

    private synchronized void packAndSend(boolean forceRequestReceipt) throws IOException {
        try {
            if(eofAtIndex!=-1 && latestAttemptedIndex+1 > eofAtIndex)
                throw new IOException("Stream closed");
//            long startCtm = System.currentTimeMillis();
            while(latestAttemptedIndex+1 >= earliestUnconfirmedPartIndex+unconfirmedSendPackages.length) {
//                System.out.println("P2LOutputStream.packAndSend - WAIT - for: "+(System.currentTimeMillis()-startCtm)/1e3+" - nextIndexToSend("+(latestAttemptedIndex+1)+"), earliestUnconfirmedPartIndex("+earliestUnconfirmedPartIndex+")");
                wait(1000); //exceeding buffer limitations - slow down!, do not send and block until packages confirmed as received
                if(latestAttemptedIndex+1 >= earliestUnconfirmedPartIndex+unconfirmedSendPackages.length)
                    sendExtraProcessReceiptRequest();
            }
            int unconfirmedBufferIndex = virtuallySend();
            send(unconfirmedBufferIndex, forceRequestReceipt);

            unsendBuffer.offset = unsendBuffer.lastDataIndex = headerSize;//clear to offset
        } catch (InterruptedException e) {
            //todo mark broken, etc...
            throw new IOException(e);
        }
    }
    private synchronized int virtuallySend() {
        latestAttemptedIndex++;

        int unconfirmedBufferIndex = latestAttemptedIndex - earliestUnconfirmedPartIndex;
        if(earliestUnconfirmedPartIndex > latestAttemptedIndex)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") < 0   [earliestUnconfirmedPartIndex= "+earliestUnconfirmedPartIndex+", latestAttemptedIndex= "+latestAttemptedIndex+"]");
        else if(unconfirmedBufferIndex >= unconfirmedSendPackages.length)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") >= unconfirmedSendPackages.length("+unconfirmedSendPackages.length+")");

        if(unconfirmedSendPackages[unconfirmedBufferIndex] == null)
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


    private synchronized void handleReceipt(Pair<Integer, int[]> receipt) {
        boolean eof = receipt.l<0;
        int latestIndexReceivedByPeer = Math.abs(receipt.l);
        int[] missingParts = receipt.r;
        if(eof)
            eofAtIndex = latestIndexReceivedByPeer;//i.e. now
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - missingParts = " + Arrays.toString(missingParts));
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - earliestUnconfirmedPartIndex = " + earliestUnconfirmedPartIndex);

        if(latestIndexReceivedByPeer+1 < earliestUnconfirmedPartIndex) {
            return;//received old receipt
        }

        int newEarliestUnconfirmedIndex;
        if(missingParts.length == 0) {
            newEarliestUnconfirmedIndex = latestIndexReceivedByPeer+1;
        } else {
            Arrays.sort(missingParts);
            newEarliestUnconfirmedIndex = missingParts[0];

//            System.out.println("1 newEarliestUnconfirmedIndex = " + newEarliestUnconfirmedIndex);

            if(newEarliestUnconfirmedIndex <= latestAttemptedIndex) {
//                System.out.println("1 newEarliestUnconfirmedIndex <= latestAttemptedIndex");
                try {
                    if (newEarliestUnconfirmedIndex < earliestUnconfirmedPartIndex) {
//                        System.out.println("RETURN newEarliestUnconfirmedIndex < earliestUnconfirmedPartIndex");
                        return; //then this receipt is not current anymore
                    }
//                    System.out.println("1 newEarliestUnconfirmedIndex >= earliestUnconfirmedPartIndex");

                    int lastMissingPart = newEarliestUnconfirmedIndex;
                    for (int missingPart : missingParts) {
                        if (newEarliestUnconfirmedIndex == -1)
                            newEarliestUnconfirmedIndex = missingPart;
                        for (int bufI = lastMissingPart + 1; bufI < missingPart; bufI++)//does not trigger the first round
                            markConfirmed(bufI);

//                        System.out.println("in for - missingPart = " + missingPart+" - isConfirmed(missingPart): "+isConfirmed(missingPart));
                        if (isConfirmed(missingPart)) {
                            if (missingPart == newEarliestUnconfirmedIndex)
                                newEarliestUnconfirmedIndex = -1;
                        } else {
                            System.out.println("resend missingPart = " + missingPart);
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
            }//else {System.out.println("ELSE newEarliestUnconfirmedIndex > latestAttemptedIndex");}// else newEarliestUnconfirmedIndex > latestAttemptedIndex : the peer notifies us that it has been waiting on a part we have not send yet - it might do this in case of delayed receipt requests
        }
        int shiftBy = newEarliestUnconfirmedIndex - earliestUnconfirmedPartIndex;
        System.arraycopy(unconfirmedSendPackages, shiftBy, unconfirmedSendPackages, 0, unconfirmedSendPackages.length - shiftBy);
        for (int i = unconfirmedSendPackages.length - shiftBy; i < unconfirmedSendPackages.length; i++)
            markConfirmed(earliestUnconfirmedPartIndex+i);
        earliestUnconfirmedPartIndex = newEarliestUnconfirmedIndex;

        notify();
    }
}