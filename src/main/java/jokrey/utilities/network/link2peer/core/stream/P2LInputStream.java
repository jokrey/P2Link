package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.DataChunk;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.util.LinkedList;

/**
 * @author jokrey
 */
class P2LInputStream extends InputStream implements AutoCloseable {
    private final P2LNodeInternal parent;
    private final SocketAddress to;
    private final int type, conversationId;
    public P2LInputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.type = type;
        this.conversationId = conversationId;
    }


    private int earliestIndexMissing = 0;//todo wrap around feature (i.e. infinite stream)
    private int latestIndexReceived = 0;//todo can be replaced with boolean(for the context in which it is currently used
    private boolean eofReceived =false;
    private int available = 0;
    private LinkedList<DataChunk> unconsumedChunksQueue = new LinkedList<>();//max size is not defined todo limit to a maximum size

    //idea: earliestIndexMissing is to be understood as the -1th element in this array
    private DataChunk[] unqueuedChunks = new DataChunk[P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE];//allows up to 64 later messages to be received, before the first message is finally required to drain this buffer

    private int[] getMissingPartIndices() {
        int count = 1;//because of earliest missing index - unless eof, the next index is always missing
        for(int i = 0; i < unqueuedChunks.length; i++) {
            if (unqueuedChunks[i] == null && (earliestIndexMissing + i + 1) <= latestIndexReceived)
                count++;
        }
        int[] missingParts = new int[count];
        missingParts[0] = earliestIndexMissing;
        int mi=1;
        for (int i = 0; i < unqueuedChunks.length && (earliestIndexMissing + i + 1) <= latestIndexReceived; i++)
            if (unqueuedChunks[i] == null)
                missingParts[mi++] = earliestIndexMissing + i + 1;
        return missingParts;
    }

    synchronized void received(P2LMessage message) {
        if(isClosed()) {
            if(message.header.requestReceipt())
                sendReceipt(); //this is to help the output stream on the other end - it might not have gotten our receipt
            return;
        }

        DataChunk unreadDataChunk = new DataChunk(message);

        if(message.header.isStreamEof())
            eofReceived =true;

        int m_index_received = message.header.getPartIndex();
        System.out.println("rec("+m_index_received+") - earliestIndexMissing = " + earliestIndexMissing);
        latestIndexReceived = Math.max(latestIndexReceived, m_index_received);

        if(m_index_received == earliestIndexMissing) {//jackpot - this is what we have been waiting for
            if(!unreadDataChunk.isEmpty()) {
                unconsumedChunksQueue.addLast(unreadDataChunk);
                available += unreadDataChunk.size();
            }
            earliestIndexMissing++;

            int index=0;
            for(;index<unqueuedChunks.length;index++) { //re-add previously received later messages
                DataChunk chunk = unqueuedChunks[index];
                if(chunk==null) break;
                if(!chunk.isEmpty()) {
                    unconsumedChunksQueue.addLast(chunk);
                    available += chunk.size();
                }
            }
            int shiftBy = index + 1;
            if(shiftBy>unqueuedChunks.length) shiftBy = unqueuedChunks.length; //todo this line feels weird
            //   todo: only recopy if it is full - i.e. keep an additional internal index and use that (index+internalOff, unqueuedIndex+internalOff)
            System.arraycopy(unqueuedChunks, shiftBy, unqueuedChunks, 0, unqueuedChunks.length - shiftBy);
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
            //delayed package REreceived... ignore it - data is already available
        } else if(m_index_received >= earliestIndexMissing+1 + unqueuedChunks.length) {
            //sender is sending more packages than can be handled by this buffer - send a receipt that indicates the earliest missing package, so that the sender can resend it
            //   typically only happens when the send buffer is larger than this receive buffer
            sendReceipt();
        } else {
            int unqueuedIndex = (m_index_received - earliestIndexMissing)-1; //min should be 0
            unqueuedChunks[unqueuedIndex] = unreadDataChunk;
        }

        if(message.header.requestReceipt()) {//has to be send after earliestIndex missing etc were updated
            sendReceipt();
        }
    }

    private void sendReceipt() {
        boolean eof = isClosed() || eofReached();
        try {
            parent.sendInternalMessage(StreamReceipt.encode(type, conversationId, eof, latestIndexReceived, getMissingPartIndices()), to);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override public int available() {
        return available;
    }
    public boolean eofReached() {
        return eofReceived && earliestIndexMissing-1 == latestIndexReceived;
    }
    public boolean isClosed() {
        return (eofReached() && available <= 0) || available==-1;
    }

    @Override public synchronized int read() throws IOException {
        try {
            while(available==0 && !eofReached()) {
                wait(1000);
                if(available==0 && !eofReached())
                    sendReceipt();
            }
            if(eofReached() && available==0) return -1;
            if(available==-1) throw new IOException("stream was closed using close");

            DataChunk unreadDataChunk = unconsumedChunksQueue.getFirst();
            byte singleByteOfData = unreadDataChunk.moveSingleByte();
            available--;
            if(unreadDataChunk.isEmpty())
                unconsumedChunksQueue.removeFirst();
            return singleByteOfData & 0xff; // & 0xff for conversion to 0-255 byte as int
        } catch (InterruptedException e) {
            throw new IOException("internal wait interrupted");
        }
    }
    @Override public synchronized int read(byte[] b, int off, int len) throws IOException {
        if(b == null) throw new NullPointerException("b == null");
        if(off<0 || off+len>b.length || len < 0) throw new ArrayIndexOutOfBoundsException();
        try {
//            long startCtm = System.currentTimeMillis();
            while(available==0 && !eofReached()) {
//                System.out.println("P2LInputStream.read-array - WAIT - eofReceived= "+eofReceived+", earliestIndexMissing= "+earliestIndexMissing+", waitedFor = " + (System.currentTimeMillis()-startCtm)/1e3);
                wait(1000);
                if(available==0 && !eofReached())
                    sendReceipt();
            }
            if(eofReached() && available==0) return -1;
            if(available==-1) throw new IOException("stream was closed using close");

            DataChunk unreadDataChunk = unconsumedChunksQueue.getFirst();

            int numRead = 0;
            while(available>0 && numRead<len) {
                int leftToRead = len-numRead;
                int remainingInChunk = unreadDataChunk.size();

                int numberOfBytesToCopy = Math.min(leftToRead, remainingInChunk);

                unreadDataChunk.moveTo(b, off+numRead, numberOfBytesToCopy);
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
        System.err.println("todo optimized skip");
        return super.skip(n);  //todo highly optimized skip, potentially with informing the other stream of the intention, by sending a receipt with a sufficiently high confirmation id (though bytes are not transferable to parts....)
    }
    @Override public synchronized void close() {
        if(unconsumedChunksQueue !=null) unconsumedChunksQueue.clear();
        unconsumedChunksQueue = null;
        available = -1;
        notify();
        sendReceipt();
    }
}