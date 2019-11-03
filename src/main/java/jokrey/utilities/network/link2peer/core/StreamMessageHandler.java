package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.HeaderIdentifier;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jokrey
 */
public class StreamMessageHandler {
    private final ConcurrentHashMap<HeaderIdentifier, P2LInputStream> map = new ConcurrentHashMap<>();

    public void received(P2LMessage message) {
        System.out.println("received stream part message = [" + message + "]");
        P2LInputStream stream = getInputStream(message);
        stream.received(message);

//        if(stream.isClosed())
//            map.remove(new SenderTypeConversationIdentifier(message));
        //todo PROBLEM: if a delayed package comes in after a request to resend a new map entry would be created
        //   solution: same last packet received time + a stream timeout feature (then this node app could control when data is cleaned up)
        //             additionally require that for a packet to be accepted,
        //                 it is required that the input stream is read from currently (i.e. someone is waiting/or rather a stream was requested and is not closed) - any other packages are disregarded
    }


    public InputStream getInputStream(SocketAddress from, int type, int conversationId) {
        return getInputStream(WhoAmIProtocol.toString(from), type, conversationId);
    }
    public InputStream getInputStream(String from, int type, int conversationId) {
        return getInputStream(new SenderTypeConversationIdentifier(from, type, conversationId));
    }
    private P2LInputStream getInputStream(P2LMessage m) {
        return getInputStream(new SenderTypeConversationIdentifier(m));
    }
    private P2LInputStream getInputStream(HeaderIdentifier identifier) {
        return map.computeIfAbsent(identifier, k -> new P2LInputStream());
    }

    private static class P2LInputStream extends InputStream {
        private int earliestIndexMissing = 0;//todo wrap around feature (i.e. infinite stream)
        private int latestIndexReceived = 0;//todo can be replaced with boolean(for the context in which it is currently used
        private boolean eofReceived =false;
        private int available = 0;
        private LinkedList<DataChunk> unconsumedChunksQueue = new LinkedList<>();

        //idea: earliestIndexMissing is to be understood as the 0th element in this array (it is always null, since it is missing (maybe replace with -1th element))
        private DataChunk[] unqueuedChunks = new DataChunk[P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE];//allows up to 64 later messages to be received, before the first message is finally required to drain this buffer


        private synchronized void received(P2LMessage message) {
            if(isClosed()) return;
            DataChunk unreadDataChunk = new DataChunk(message);

            if(message.header.isStreamEof())
                eofReceived =true;

//            System.out.println("unconsumedChunksQueue before = " + unconsumedChunksQueue);
            int m_index_received = message.header.getPartIndex();
            latestIndexReceived = Math.max(latestIndexReceived, m_index_received);
            if(m_index_received == earliestIndexMissing) {
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
                System.arraycopy(unqueuedChunks, shiftBy, unqueuedChunks, 0, unqueuedChunks.length - shiftBy);
                for(int i=unqueuedChunks.length-shiftBy;i<unqueuedChunks.length;i++)
                    unqueuedChunks[i]=null;

                //   todo: only recopy if it is full - i.e. keep an additional internal index and use that (index+internalOff, unqueuedIndex+internalOff)
                earliestIndexMissing = earliestIndexMissing + index;
            } else {
//                System.out.println("x - earliestIndexMissing = " + earliestIndexMissing);
//                System.out.println("x - latestIndexReceived = " + latestIndexReceived);
//                System.out.println("x - m_index_received = " + m_index_received);
                int unqueuedIndex = (m_index_received - earliestIndexMissing)-1; //min should be 1
//                System.out.println("x - unqueuedIndex = " + unqueuedIndex);
                if(unqueuedIndex<0) {
                    System.err.println("unqueuedIndex("+unqueuedIndex+") < 0 - this cannot happen (except maybe for resend packages, in which case this can be easily ignored)");
                    return;
                }
                if(unqueuedIndex >= unqueuedChunks.length) {
                    //todo - at this point the package has to be dropped intentionally..
                    //todo - send a receipt with a delay request and a list of missing indices

                    //todo - basic logic:
                    //  sender sends a self chosen number n of packages(but keeps them in memory for now) - HAS TO BE BLOCKING, except when the user provides additionally buffer memory
                    //  receiver receives as many packages as possible with the algorithm above
                    //     when the buffer is full, - i.e. the earliest not received package has been a while ago(either lost or congestion[IS THAT REALLY WHAT CONGESTION MEANS])
                    //       then send a receipt of currently missing package indices back to sender [[OR MARK 'FULL' AND WAIT A WHILE LONGER;WHILE CONTINUING TO DROP PACKAGES FOR A SECOND?? try, maybe]]
                    //           note: no hashes required - udp checksum reasonably guarantees that every received package was valid
                    //       MAYBE: flag bit of whether the response means missing or received package indices - depending on what is more..
                    //  sender receives receipt with list of currently missing package indices
                    //     sender will clear buffer of packages the receipt indicates as received
                    //     all other packages will be resend - in addition to new packages made available by the user
                    throw new IllegalStateException("what to do if unqueued message bounds are hit");
                }

                unqueuedChunks[unqueuedIndex] = unreadDataChunk;
            }

            if(available > 0 || eofReached()) {
                notifyAll();
            }

            System.out.println("eofReceived = " + eofReceived);
//            System.out.println("eofReached() = " + eofReached());
//            System.out.println("earliestIndexMissing = " + earliestIndexMissing);
//            System.out.println("latestIndexReceived = " + latestIndexReceived);
            System.out.println("m_index_received = " + m_index_received);
//            System.out.println("available = " + available);
//            System.out.println("unreadDataChunk = " + unreadDataChunk);
//            System.out.println("unconsumedChunksQueue after = " + unconsumedChunksQueue);
//            System.out.println("unqueuedChunks = " + Arrays.toString(unqueuedChunks));
//            System.out.println();
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
                    wait();
                }
                if(eofReached() && available==0) return -1;
                if(available==-1) throw new IOException("stream was closed using close");

                DataChunk unreadDataChunk = unconsumedChunksQueue.getFirst();
                byte singleByteOfData = unreadDataChunk.data[unreadDataChunk.offset];
                unreadDataChunk.offset++;
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
                while(available==0 && !eofReached()) {
                    wait();
                }
                if(eofReached() && available==0) return -1;
                if(available==-1) throw new IOException("stream was closed using close");

                DataChunk unreadDataChunk = unconsumedChunksQueue.getFirst();

//                System.out.println("read before while");
//                System.out.println("len = " + len);
                int numRead = 0;
                while(available>0 && numRead<len) {
//                    System.out.println("read while-start unreadDataChunk = " + unreadDataChunk);
//                    System.out.println("read while-start available = " + available);

                    int leftToRead = len-numRead;
                    int remainingInChunk = unreadDataChunk.size();

                    int numberOfBytesToCopy = Math.min(leftToRead, remainingInChunk);

//                    System.out.println("numRead = " + numRead);
//                    System.out.println("leftToRead = " + leftToRead);
//                    System.out.println("remainingInChunk = " + remainingInChunk);
//                    System.out.println("numberOfBytesToCopy = " + numberOfBytesToCopy);

                    System.arraycopy(unreadDataChunk.data, unreadDataChunk.offset, b, off+numRead, numberOfBytesToCopy);
                    numRead+=numberOfBytesToCopy;
                    unreadDataChunk.offset+=numberOfBytesToCopy;
                    available-=numberOfBytesToCopy;

//                    System.out.println("read while-mid unreadDataChunk = " + unreadDataChunk);
//                    System.out.println("read while-mid available = " + available);
//                    System.out.println("read while-mid numRead = " + numRead);
//                    System.out.println("read while-mid unreadDataChunk.isEmpty() = " + unreadDataChunk.isEmpty());

                    if(unreadDataChunk.isEmpty()) {
//                        System.out.println("current chunk empty - move to next chunk");
//                        System.out.println("unconsumedChunksQueue = " + unconsumedChunksQueue);
                        DataChunk removed = unconsumedChunksQueue.removeFirst();
//                        System.out.println("removed = " + removed);
//                        System.out.println("unconsumedChunksQueue after = " + unconsumedChunksQueue);
                        unreadDataChunk = unconsumedChunksQueue.peekFirst();
//                        System.out.println("new unreadDataChunk = " + unreadDataChunk);
                        //NO NEED TO CHECK IF UNREAD DATA CHUNK IS NULL AND BREAK ACCORDINGLY - available SHOULD BE EXACTLY 0 THEN
                    }
//                    System.out.println("-");
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
        }


        @Override public int read(byte[] b) throws IOException {
            return super.read(b); //reasonable super implementation
        }
    }

    private static class DataChunk {
        public final byte[] data;
        public int offset;
        public final int lastDataIndex;
        public DataChunk(P2LMessage from) {
            this(from.raw, from.header.getSize(), from.payloadLength);
        }
        public DataChunk(byte[] data, int offset, int dataLen) {
//            this.data = data;
//            this.offset = offset;
//            this.lastDataIndex = offset+dataLen;

            this.data = Arrays.copyOfRange(data, offset, offset+dataLen);
            this.offset=0;
            this.lastDataIndex = dataLen;
        }
        public int size() {
            return lastDataIndex-offset;
        }
        public boolean isEmpty() {
            return lastDataIndex==offset;
        }

        @Override public String toString() {
            return "DataChunk{" + "offset=" + offset + ", lastDataIndex=" + lastDataIndex + ", data=" + Arrays.toString(data) + '}';
        }
    }
}
