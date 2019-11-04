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

    //idea: earliestIndexMissing is to be understood as the 0th element in this array (it is always null, since it is missing (maybe replace with -1th element))
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
//        System.out.println("getMissingPartIndices - earliestIndexMissing = " + earliestIndexMissing);
//        System.out.println("getMissingPartIndices - missingParts = " + Arrays.toString(missingParts));
//        System.out.println("getMissingPartIndices - unqueuedChunks = " + Arrays.toString(unqueuedChunks));
        return missingParts;
    }

    synchronized void received(P2LMessage message) {
        if(isClosed()) {
            if(message.header.requestReceipt())
                sendReceipt();
            return;
        }

        DataChunk unreadDataChunk = new DataChunk(message);

        if(message.header.isStreamEof())
            eofReceived =true;

//            ////System.out.println("unconsumedChunksQueue before = " + unconsumedChunksQueue);
        int m_index_received = message.header.getPartIndex();
        latestIndexReceived = Math.max(latestIndexReceived, m_index_received);
        System.out.println("rec("+m_index_received+") - earliestIndexMissing = " + earliestIndexMissing);
//        System.out.println("rec("+m_index_received+") - m_index_received = " + m_index_received);
//        System.out.println("rec - latestIndexReceived = " + latestIndexReceived);
        //System.out.println("rec - unconsumedChunksQueue = " + unconsumedChunksQueue);
//        System.out.println("rec("+m_index_received+") - unreadDataChunk = " + unreadDataChunk);
//        System.out.println("rec("+m_index_received+") - unqueuedChunks before = " + Arrays.toString(unqueuedChunks));
        if(m_index_received == earliestIndexMissing) {
//            System.out.println("hit == ");
            //jackpot - this is what we have been waiting for

            if(!unreadDataChunk.isEmpty()) {
                unconsumedChunksQueue.addLast(unreadDataChunk);
                available += unreadDataChunk.size();
            }
            earliestIndexMissing++;
//                unqueuedChunks[0] = unreadDataChunk;

            //re-add previously received later messages
            int index=0;
            for(;index<unqueuedChunks.length;index++) {
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
//            System.out.println("in stream received shiftBy = " + shiftBy);
            System.arraycopy(unqueuedChunks, shiftBy, unqueuedChunks, 0, unqueuedChunks.length - shiftBy);
            for(int i=unqueuedChunks.length-shiftBy;i<unqueuedChunks.length;i++)
                unqueuedChunks[i]=null;

            earliestIndexMissing = earliestIndexMissing + index;

//            System.out.println("rec("+m_index_received+") message = " + message);

            if(available > 0 || eofReached()) {
                notifyAll();
            }

//            if(message.header.requestReceipt())//first things first, when the sender requests a receipt - that means that the sender buffer is full - we should send them a receipt of what we have and do not have immediately
//                sendReceipt(false);
//            System.out.println("rec("+m_index_received+") - unqueuedChunks before = " + Arrays.toString(unqueuedChunks));
        } else if(m_index_received==-1) {
            //the sender sends this in case it does not receive a receipt for a send package - for example when said package or the receipt is dropped
            //    only relevant if the sender does not send any subsequent packages that would make the loss apparent
            if(!message.header.requestReceipt())//first things first, when the sender requests a receipt - that means that the sender buffer is full - we should send them a receipt of what we have and do not have immediately
                throw new IllegalStateException("message had negative index, but was not requesting receipt");
        } else if(m_index_received < earliestIndexMissing) {
            //delayed package REreceived... ignore it - data is already available
        } else if(m_index_received >= earliestIndexMissing+1 + unqueuedChunks.length) {
            //sender is sending more packages than can be handled by this buffer - send a receipt that indicates the earliest missing package, so that the sender can resend it
            //   typically only happens when the send buffer is
//            System.out.println("TRANSMIT RECEIPT m_index_received("+m_index_received+") > earliestIndexMissing("+earliestIndexMissing+") + 1 + unqueuedChunks.length("+unqueuedChunks.length+")");
            sendReceipt();
        } else {
            int unqueuedIndex = (m_index_received - earliestIndexMissing)-1; //min should be 0
//            System.out.println("rec("+m_index_received+") before unqueuedChunks[unqueuedIndex("+unqueuedIndex+")] = " + unqueuedChunks[unqueuedIndex]);
            unqueuedChunks[unqueuedIndex] = unreadDataChunk;
        }


        if(message.header.requestReceipt()) {//has to be send after earliestIndex missing etc were updated
            sendReceipt();
        }


//        //System.out.println("eofReceived = " + eofReceived);
            ////System.out.println("eofReached() = " + eofReached());
//            //System.out.println("earliestIndexMissing = " + earliestIndexMissing);
//            ////System.out.println("latestIndexReceived = " + latestIndexReceived);
//        //System.out.println("m_index_received = " + m_index_received);
//            ////System.out.println("available = " + available);
//            ////System.out.println("unreadDataChunk = " + unreadDataChunk);
        //System.out.println("rec - earliestIndexMissing after = " + earliestIndexMissing);
            //System.out.println("rec - unconsumedChunksQueue after = " + unconsumedChunksQueue);
            //System.out.println("rec - unqueuedChunks after = " + Arrays.toString(unqueuedChunks));
//            ////System.out.println();
    }

    private void sendReceipt() {
//        Thread.dumpStack();
        boolean eof = isClosed() || eofReached();
        try {
            parent.sendInternalMessage(StreamReceipt.encode(type, conversationId, eof, latestIndexReceived, getMissingPartIndices()), to);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
//                System.out.println("P2LInputStream.read-int - WAIT");
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
                if(available==0 && !eofReached()) {
//                    System.out.println("P2LInputStream.read-array - WAIT - retransmit receipt");
                    sendReceipt();
                }
            }
//            System.out.println("P2LInputStream.read-array total waitedFor = " + (System.currentTimeMillis()-startCtm)/1e3);
            if(eofReached() && available==0) return -1;
            if(available==-1) throw new IOException("stream was closed using close");

            DataChunk unreadDataChunk = unconsumedChunksQueue.getFirst();

//                //System.err.println("read before while");
//                //System.err.println("len = " + len);
            int numRead = 0;
            while(available>0 && numRead<len) {
//                    //System.err.println("read while-start unreadDataChunk = " + unreadDataChunk);
//                    //System.err.println("read while-start available = " + available);

                int leftToRead = len-numRead;
                int remainingInChunk = unreadDataChunk.size();

                int numberOfBytesToCopy = Math.min(leftToRead, remainingInChunk);

//                    //System.err.println("numRead = " + numRead);
//                    //System.err.println("leftToRead = " + leftToRead);
//                    //System.err.println("remainingInChunk = " + remainingInChunk);
//                    //System.err.println("numberOfBytesToCopy = " + numberOfBytesToCopy);

                unreadDataChunk.moveTo(b, off+numRead, numberOfBytesToCopy);
                numRead+=numberOfBytesToCopy;
                available-=numberOfBytesToCopy;

//                    //System.err.println("read while-mid unreadDataChunk = " + unreadDataChunk);
//                    //System.err.println("read while-mid available = " + available);
//                    //System.err.println("read while-mid numRead = " + numRead);
//                    //System.err.println("read while-mid unreadDataChunk.isEmpty() = " + unreadDataChunk.isEmpty());

                if(unreadDataChunk.isEmpty()) {
//                        //System.err.println("current chunk empty - move to next chunk");
//                        //System.err.println("unconsumedChunksQueue = " + unconsumedChunksQueue);
                    unconsumedChunksQueue.removeFirst();
                        ////System.out.println("removed = " + removed);
//                        //System.err.println("unconsumedChunksQueue after = " + unconsumedChunksQueue);
                    unreadDataChunk = unconsumedChunksQueue.peekFirst();
//                        //System.err.println("new unreadDataChunk = " + unreadDataChunk);
                    //NO NEED TO CHECK IF UNREAD DATA CHUNK IS NULL AND BREAK ACCORDINGLY - available SHOULD BE EXACTLY 0 THEN
                }
//                    //System.err.println("-");
            }
            if(available<0) throw new IllegalStateException("available cannot go under 0 without a bug");

            return numRead; // & 0xff for conversion to 0-255 byte as int
        } catch (InterruptedException e) {
            throw new IOException("internal wait interrupted");
        }
    }
    @Override public long skip(long n) throws IOException {
        System.err.println("todo");
        return super.skip(n);
    }
    @Override public int available() {
        return available;
    }
    @Override public synchronized void close() {
        if(unconsumedChunksQueue !=null) unconsumedChunksQueue.clear();
        unconsumedChunksQueue = null;
        available = -1;
        notifyAll();
        sendReceipt();
    }


//    @Override public int read(byte[] b) throws IOException {
//        return super.read(b); //reasonable super implementation
//    }
}