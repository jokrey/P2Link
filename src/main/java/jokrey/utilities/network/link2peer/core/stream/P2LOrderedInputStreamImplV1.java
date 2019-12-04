package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.network.link2peer.util.TimeoutException;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.LinkedList;

/**
 * TODO - should itself decide when to send receipts, the current system allows an exploit where many small packages request receipts
 *
 * FIXME GENERAL DOWNSIDE COMPARED TO TCP:
 *    MTP IS LIMITED by custom max raw size of a message (for each incoming udp message a buffer must be reserved - that buffer should be as small as possible)
 *    currently this is defaulted at 8192 - tcp can set this buffer to the max ip package size...
 *    in a distributed environment mtp is naturally limited by other factors, but localhost this is a huge performance decrease
 *
 * @author jokrey
 */
class P2LOrderedInputStreamImplV1 extends P2LOrderedInputStream {
    P2LOrderedInputStreamImplV1(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        super(parent, to, type, conversationId);
    }

    private int earliestIndexMissing = 0;//todo wrap around feature (i.e. infinite stream)
    private int latestIndexReceived = 0;
    private boolean eofReceived =false;
    private int available = 0;
    private LinkedList<SubBytesStorage> unconsumedChunksQueue = new LinkedList<>();//max size is not defined todo limit to a maximum size (i.e. if no one is reading from the stream at some point it will stop receiving)

    //idea: earliestIndexMissing is to be understood as the -1th element in this array
    private SubBytesStorage[] unqueuedChunks = new SubBytesStorage[P2LHeuristics.ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE];//allows up to 64 later messages to be received, before the first message is finally required to drain this buffer

    @Override public synchronized void received(P2LMessage message) {
        if(isClosed()) {
            if(message.header.requestReceipt())
                sendReceipt(); //this is to help the output stream on the other end - it might not have gotten our receipt
            return;
        }

        SubBytesStorage unreadDataChunk = message.payload();

        if(message.header.isStreamEof())
            eofReceived =true;

        int m_index_received = message.header.getPartIndex();
        System.out.println("rec("+m_index_received+") - earliestIndexMissing = " + earliestIndexMissing);
        latestIndexReceived = Math.max(latestIndexReceived, m_index_received);

        if(m_index_received == earliestIndexMissing) {//jackpot - this is what we have been waiting for
            if(!unreadDataChunk.isEmpty()) {
                unconsumedChunksQueue.addLast(unreadDataChunk);
                available += unreadDataChunk.contentSize();
            }
            earliestIndexMissing++;

            int index=0;
            for(;index<unqueuedChunks.length;index++) { //re-add previously received later messages
                SubBytesStorage chunk = unqueuedChunks[index];
                if(chunk==null) break;
                if(!chunk.isEmpty()) {
                    unconsumedChunksQueue.addLast(chunk);
                    available += chunk.contentSize();
                }
            }
            int shiftBy = index + 1;
            if(shiftBy>unqueuedChunks.length) shiftBy = unqueuedChunks.length; //todo this line feels weird
            //   not_todo: only recopy if it is full - i.e. keep an additional internal index and use that (index+internalOff, unqueuedIndex+internalOff)
            System.arraycopy(unqueuedChunks, shiftBy, unqueuedChunks, 0, unqueuedChunks.length - shiftBy);//no rotate required, new packages always have new pointers
            for(int i=unqueuedChunks.length-shiftBy;i<unqueuedChunks.length;i++)
                unqueuedChunks[i]=null;

            earliestIndexMissing = earliestIndexMissing + index;

            if(available > 0 || eofReached())
                notify();
        } else if(m_index_received==-1) {
            //the sender sends this in case it does not receive a receipt for a send package - for example when said package or the receipt is dropped
            //    only relevant if the sender does not send any subsequent packages that would make the loss apparent
            if(!message.header.requestReceipt())//first things first, when the sender requests a receipt - that means that the sender buffer is full - we should send them a receipt of what we have and do not have immediately
                throw new IllegalStateException("message had negative index, but was not requesting receipt");
        } else if(m_index_received < earliestIndexMissing) {
            //delayed package RE-received... ignore it - data is already available
        } else if(m_index_received >= earliestIndexMissing+1 + unqueuedChunks.length) {
            //sender is sending more packages than can be handled by this buffer - send a receipt that indicates the earliest missing package, so that the sender can resend it
            //   typically only happens when the send buffer is larger than this receive buffer
            sendReceipt();
        } else {
            int unqueuedIndex = (m_index_received - earliestIndexMissing)-1; //min should be 0
            unqueuedChunks[unqueuedIndex] = unreadDataChunk;

            if(unqueuedIndex == unqueuedChunks.length-unqueuedChunks.length/3) //if buffer 66.6% filled up, send extraordinary receipt
//            if(unqueuedIndex == unqueuedChunks.length/2) //if buffer 50% filled up, send extraordinary receipt
                sendReceipt();
        }

        if(message.header.requestReceipt()) {//has to be send after earliestIndex missing etc were updated
            sendReceipt();
        }
    }


    private long lastSendReceipt = -1;
    private int lastEarliestMissing = -1;
    private void sendReceipt() {
        long now = System.currentTimeMillis();
        if(lastEarliestMissing==earliestIndexMissing) {
            if (lastSendReceipt != -1 && (now - lastSendReceipt) <= P2LHeuristics.ORDERED_STREAM_CHUNK_RECEIPT_RESEND_LIMITATION_IN_MS)
                return;
        }
        lastSendReceipt = now;
        lastEarliestMissing=earliestIndexMissing;


        boolean eof = isClosed() || eofReached();
        try {
            parent.sendInternalMessage(P2LOrderedStreamReceipt.encode(type, conversationId, eof, latestIndexReceived, getMissingPartIndices()), to);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override public int available() {
        return available;
    }
    private boolean eofReached() {
        return eofReceived && earliestIndexMissing-1 == latestIndexReceived;
    }
    @Override public boolean isClosed() {
        return (eofReached() && available <= 0) || available==-1;
    }

    @Override public synchronized int read(int timeout_ms) throws IOException {
        try {
            boolean success = SyncHelp.waitUntil(this, () -> available!=0 || eofReached(), timeout_ms);
            if(!success) throw new IOException("timeout");
            if(eofReached() && available==0) return -1;
            if(available==-1) throw new IOException("stream was closed using close");

            SubBytesStorage unreadDataChunk = unconsumedChunksQueue.getFirst();
            byte singleByteOfData = unreadDataChunk.getFirst();
            unreadDataChunk.startIndexAdd(1);
            available--;
            if(unreadDataChunk.isEmpty())
                unconsumedChunksQueue.removeFirst();
            return singleByteOfData & 0xff; // & 0xff for conversion to 0-255 byte as int
        } catch (InterruptedException e) {
            throw new IOException("internal wait interrupted");
        }
    }
    @Override public synchronized int read(byte[] b, int off, int len, int timeout_ms) throws IOException {
        if(b == null) throw new NullPointerException("b == null");
        if(off<0 || off+len>b.length || len < 0) throw new ArrayIndexOutOfBoundsException();
        try {
            boolean success = SyncHelp.waitUntil(this, () -> available!=0 || eofReached(), timeout_ms);
            if(!success) throw new IOException("timeout");
            if(eofReached() && available==0) return -1;
            if(available==-1) throw new IOException("stream was closed using close");

            SubBytesStorage unreadDataChunk = unconsumedChunksQueue.getFirst();

            int numRead = 0;
            while(available>0 && numRead<len) {
                int leftToRead = len-numRead;
                int remainingInChunk = (int) unreadDataChunk.contentSize();

                int numberOfBytesToCopy = Math.min(leftToRead, remainingInChunk);

                unreadDataChunk.copyInto(b, off+numRead, numberOfBytesToCopy);
                unreadDataChunk.startIndexAdd(numberOfBytesToCopy);
                numRead+=numberOfBytesToCopy;
                available-=numberOfBytesToCopy;

                if(unreadDataChunk.isEmpty()) {
                    unconsumedChunksQueue.removeFirst();
                    unreadDataChunk = unconsumedChunksQueue.peekFirst();
                    //NO NEED TO CHECK IF UNREAD DATA CHUNK IS NULL AND BREAK ACCORDINGLY - available SHOULD BE EXACTLY 0 THEN - OTHERWISE WE HAVE A LARGER ISSUE
                }
            }
            if(available<0) throw new IllegalStateException("available cannot go under 0 without a bug");

            return numRead; // & 0xff for conversion to 0-255 byte as int
        } catch (InterruptedException e) {
            throw new IOException("internal wait interrupted");
        }
    }
    @Override public long skip(long n) throws IOException {
        System.err.println("todo optimized skip in which unreceived intermediate packages are ignored and not waited upon");
        return super.skip(n);  //todo highly optimized skip, potentially with informing the other stream of the intention, by sending a receipt with a sufficiently high confirmation id (though bytes are not transferable to parts....)
    }
    @Override public synchronized void close() {
        if(unconsumedChunksQueue !=null) unconsumedChunksQueue.clear();
        unconsumedChunksQueue = null;
        available = -1;
        notify();
        sendReceipt();
        //todo remove this object from the stream message handler - do not remove if it is closed from the other side tho - force close for resource destruction
    }


    private int[] getMissingPartIndices() {
        int count = 1;//because of earliest missing index - unless eof, the next index is always missing
        for(int i = 0; i < unqueuedChunks.length; i++)
            if (unqueuedChunks[i] == null && (earliestIndexMissing + i + 1) <= latestIndexReceived)
                count++;
        int[] missingParts = new int[count];
        missingParts[0] = earliestIndexMissing;
        int mi=1;
        for (int i = 0; i < unqueuedChunks.length && (earliestIndexMissing + i + 1) <= latestIndexReceived; i++)
            if (unqueuedChunks[i] == null)
                missingParts[mi++] = earliestIndexMissing + i + 1;
        return missingParts;
    }
}