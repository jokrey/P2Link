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

    private final DataChunk[] unconfirmedSendPackages = new DataChunk[P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE + 1/*+1*/]; //technically + 1, because the receiving input stream does not store the first unreceived package
//    private int remainingUnconfirmedBufferCapacity = unconfirmedSendPackages.length-1;
    private synchronized boolean isConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        return unconfirmedSendPackages[bufferIndex] == null || unconfirmedSendPackages[bufferIndex].offset==-1;
    }
    private synchronized void markConfirmed(int partIndex) {
        int bufferIndex = partIndex - earliestUnconfirmedPartIndex;
        if(unconfirmedSendPackages[bufferIndex] != null)
            unconfirmedSendPackages[bufferIndex].offset=-1;
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
        unsendBuffer.put(b, off, len);
        if(unsendBuffer.isFull())
            packAndSend(false);
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
//            System.out.println("P2LOutputStream.packAndSend - total wait: "+(System.currentTimeMillis()-startCtm)/1e3);
            int unconfirmedBufferIndex = virtuallySend();
            send(unconfirmedBufferIndex, forceRequestReceipt);

            unsendBuffer.offset = unsendBuffer.lastDataIndex = headerSize;//clear to offset
        } catch (InterruptedException e) {
            //todo mark broken, etc...
            throw new IOException(e);
        }

        ////System.out.println("packAndSend after - unsendBuffer = " + unsendBuffer);
        ////System.out.println("packAndSend after - unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));
    }
    private synchronized int virtuallySend() {
        ////System.err.println("virtuallySend - unsendBuffer = " + unsendBuffer);
        ////System.err.println("virtuallySend - unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));
        ////System.err.println("virtuallySend - earliestUnconfirmedPartIndex = " + earliestUnconfirmedPartIndex);
//        int earliestUnconfirmedPartIndex = Math.min(this.earliestUnconfirmedPartIndex, header.index);
        latestAttemptedIndex++;
        //System.out.println("virtSend("+latestAttemptedIndex+") - earliestUnconfirmedPartIndex = " + earliestUnconfirmedPartIndex);
        //System.out.println("virtSend("+latestAttemptedIndex+") - latestAttemptedIndex = " + latestAttemptedIndex);

        int unconfirmedBufferIndex = latestAttemptedIndex - earliestUnconfirmedPartIndex;
        //System.out.println("virtSend("+latestAttemptedIndex+") - unconfirmedBufferIndex = " + unconfirmedBufferIndex);
        if(earliestUnconfirmedPartIndex > latestAttemptedIndex) {
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") < 0   [earliestUnconfirmedPartIndex= "+earliestUnconfirmedPartIndex+", latestAttemptedIndex= "+latestAttemptedIndex+"]");
        }
        if(unconfirmedBufferIndex >= unconfirmedSendPackages.length)
            throw new IllegalStateException("unconfirmedBufferIndex("+unconfirmedBufferIndex+") >= unconfirmedSendPackages.length("+unconfirmedSendPackages.length+")");

        //System.out.println("virtSend("+latestAttemptedIndex+") - unconfirmedSendPackages 1 = " + Arrays.toString(unconfirmedSendPackages));
        if(unconfirmedSendPackages[unconfirmedBufferIndex] == null)
            unconfirmedSendPackages[unconfirmedBufferIndex] = new DataChunk(unsendBuffer.capacity());
        //System.out.println("virtSend("+latestAttemptedIndex+") - unconfirmedSendPackages 2 = " + Arrays.toString(unconfirmedSendPackages));
        unsendBuffer.cloneInto(unconfirmedSendPackages[unconfirmedBufferIndex]);
        //System.out.println("virtSend("+latestAttemptedIndex+") - unconfirmedSendPackages 3 = " + Arrays.toString(unconfirmedSendPackages));
//        remainingUnconfirmedBufferCapacity--;
        //System.out.println("virtSend("+latestAttemptedIndex+") - unsendBuffer = " + unsendBuffer);
        //System.out.println("virtSend("+latestAttemptedIndex+") - unconfirmedSendPackages[unconfirmedBufferIndex = " + unconfirmedSendPackages[unconfirmedBufferIndex]);
        return latestAttemptedIndex;
    }
    private synchronized void send(int partIndexToSend, boolean forceRequestReceipt) throws IOException {
        int bufferIndexToSend = partIndexToSend - earliestUnconfirmedPartIndex;
        if(bufferIndexToSend < 0) throw new IllegalStateException("buffer index("+bufferIndexToSend+") negative - this should not happen");
        if(bufferIndexToSend >= unconfirmedSendPackages.length) throw new IllegalStateException("buffer index("+bufferIndexToSend+") positive out of bounds("+unconfirmedSendPackages.length+") - this should not happen");
        DataChunk chunk = unconfirmedSendPackages[bufferIndexToSend];
//        //System.out.println("send iinternal=("+bufferIndexToSend+") chunk = " + chunk);
        //System.out.println("send("+partIndexToSend+") chunk = " + chunk);//todo - problem appears to be that:
        //for example index 4 is missing in input stream...
        // so it sends that in the receipt..
        // then this output stream supplies this method with the number 4
        // however for SOME reason unconfirmedSendPackages has an old state and sends a different message (for example index 1 message - which was never missing in input stream)

        boolean eof = eofAtIndex == partIndexToSend;
        boolean thisPartIsLastInUnconfirmedBuffer = partIndexToSend + 1 >= earliestUnconfirmedPartIndex + unconfirmedSendPackages.length;
        boolean requestReceipt = eofAtIndex!=-1 || forceRequestReceipt || eof || thisPartIsLastInUnconfirmedBuffer; //eof always requests a receipt, because the close method will not
        try {
//        boolean requestReceipt = thisPartIsLastInUnconfirmedBuffer;//todo comment out again etc
            StreamPartHeader header = new StreamPartHeader(null, type, conversationId, partIndexToSend, requestReceipt, eof);
            if(requestReceipt)
                lastReceiptRequest=System.currentTimeMillis();
            header.writeTo(chunk.data);
            parent.sendInternalMessage(new P2LMessage(header, null, chunk.data, chunk.lastDataIndex - headerSize, null), to);
            //todo - determine: is the buffer available again now(like change are not reflected in the send package)?? It fucking better be... java doc does not mention anything
        } catch (IOException e) {
            System.err.println("earliestUnconfirmedPartIndex = " + earliestUnconfirmedPartIndex);
            System.err.println("latestAttemptedIndex = " + latestAttemptedIndex);
            System.err.println("unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));
            System.err.println("partIndexToSend = " + partIndexToSend);
            System.err.println("requestReceipt = " + requestReceipt);
            System.err.println("eof = " + eof);
            e.printStackTrace();
            throw e;
        }
    }


    private synchronized void handleReceipt(Pair<Integer, int[]> receipt) {
        //Todo unconfirmed send packages is often in a wrong state - currently tests show it to only contain default values at all times
        //todo some how - on virtualSend - the ENTIRE BUFFER is

        boolean eof = receipt.l<0;
        int latestIndexReceivedByPeer = Math.abs(receipt.l);
        int[] missingParts = receipt.r;
        if(eof)
            eofAtIndex = latestIndexReceivedByPeer;//i.e. now
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - latestReceived = " + latestIndexReceivedByPeer);
        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - missingParts = " + Arrays.toString(missingParts));
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - earliestUnconfirmedPartIndex = " + earliestUnconfirmedPartIndex);
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - latestAttemptedIndex = " + latestAttemptedIndex);
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - eofAtIndex = " + eofAtIndex);
//        System.out.println("handleReceipt("+latestIndexReceivedByPeer+") - unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));

        if(latestIndexReceivedByPeer+1 < earliestUnconfirmedPartIndex) {
            //received old receipt
            //System.out.println("handleReceipt("+latestConfirmed+") - is old");
            return;
        }
        ////System.out.println("handleReceipt - unsendBuffer = " + unsendBuffer);
        ////System.out.println("handleReceipt - unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));
        int newEarliestUnconfirmedIndex;
        if(missingParts.length == 0) {
            newEarliestUnconfirmedIndex = latestIndexReceivedByPeer+1;
        } else {
            Arrays.sort(missingParts);
            ////System.out.println("missingParts sorted = " + Arrays.toString(missingParts));
            newEarliestUnconfirmedIndex = missingParts[0];

            if(newEarliestUnconfirmedIndex > latestAttemptedIndex) {//the peer notifies us that it has been waiting on a part we have not send yet - it might do this in case of delayed receipt requests
                earliestUnconfirmedPartIndex = newEarliestUnconfirmedIndex;
                notifyAll();
                return;
            }

            try {

                if (newEarliestUnconfirmedIndex < earliestUnconfirmedPartIndex)
                    return; //then this receipt is not current anymore

                int lastMissingPart = newEarliestUnconfirmedIndex;
                for (int missingPart : missingParts) {
                    if(newEarliestUnconfirmedIndex==-1)
                        newEarliestUnconfirmedIndex = missingPart;
                    for (int bufI = lastMissingPart+1; bufI < missingPart; bufI++)//does not trigger the first round
                        markConfirmed(bufI);

                    if(isConfirmed(missingPart)) {
                        if(missingPart == newEarliestUnconfirmedIndex)
                            newEarliestUnconfirmedIndex = -1;
                    } else {
                        send(missingPart, false);
                    }
                    lastMissingPart = missingPart;
                }
                if(newEarliestUnconfirmedIndex == -1)
                    newEarliestUnconfirmedIndex = latestIndexReceivedByPeer+1;

            } catch (IOException e) {
                e.printStackTrace();
                //todo mark broken, etc...
            }
        }
        int shiftBy = newEarliestUnconfirmedIndex - earliestUnconfirmedPartIndex;
        //System.out.println("handleReceipt("+latestConfirmed+") newEarliestUnconfirmedIndex = " + newEarliestUnconfirmedIndex);
        //System.out.println("handleReceipt("+latestConfirmed+") shiftBy = " + shiftBy);
//        try {
            System.arraycopy(unconfirmedSendPackages, shiftBy, unconfirmedSendPackages, 0, unconfirmedSendPackages.length - shiftBy);
//        } catch (Throwable t) {
            //System.err.println("handleReceipt("+latestConfirmed+") t = " + t);
            //System.err.println("handleReceipt("+latestConfirmed+") shiftBy = " + shiftBy);
            //System.err.println("handleReceipt("+latestConfirmed+") unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));
            //System.err.println("handleReceipt("+latestConfirmed+") unconfirmedSendPackages.length = " + unconfirmedSendPackages.length);
//            throw t;
//        }
        for (int i = unconfirmedSendPackages.length - shiftBy; i < unconfirmedSendPackages.length; i++)
            unconfirmedSendPackages[i] = null; //to mark as unconfirmed... todo is this required?? they are overriden without check, right?
//                remainingUnconfirmedBufferCapacity += shiftBy;
        earliestUnconfirmedPartIndex = newEarliestUnconfirmedIndex;

        notifyAll();

        //System.out.println("handleReceipt("+latestConfirmed+") after - earliestUnconfirmedPartIndex = " + earliestUnconfirmedPartIndex);
        //System.out.println("handleReceipt("+latestConfirmed+") after - unconfirmedSendPackages = " + Arrays.toString(unconfirmedSendPackages));
        ////System.out.println("aft remainingUnconfirmedBufferCapacity = " + remainingUnconfirmedBufferCapacity);
    }

//    @Override public void write(byte[] b) throws IOException {
//        super.write(b); //reasonable super implementation
//    }
}
