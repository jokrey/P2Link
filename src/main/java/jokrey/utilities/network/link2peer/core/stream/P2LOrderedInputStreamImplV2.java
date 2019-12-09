package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * todo - ask output stream to slow down if data is not being read...
 *
 * @author jokrey
 */
public class P2LOrderedInputStreamImplV2 extends P2LOrderedInputStream {
    private final P2LFragmentInputStreamImplV1 underlyingFragmentStream;

    private long firstIndexInBufferRepresents = 0;
    private int firstValidIndexInBuffer = 0;
    private int available = 0;
    private byte[] buffer = new byte[P2LHeuristics.ORDERED_STREAM_V2_MAX_BUFFER_SIZE];

    protected P2LOrderedInputStreamImplV2(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId) {
        super(parent, to, type, conversationId);
        underlyingFragmentStream = new P2LFragmentInputStreamImplV1(parent, to, con, type, conversationId);
        underlyingFragmentStream.addFragmentReceivedListener((fragmentOffset, receivedRaw, dataOff, dataLen, eof) -> {
//            AverageCallTimeMarker.mark_call_start("receiver");

//            System.err.println("before fragmentOffset = " + fragmentOffset + ", receivedRaw = " + new String(receivedRaw, dataOff, dataLen, StandardCharsets.UTF_8) + ", dataOff = " + dataOff + ", dataLen = " + dataLen);
            synchronized (this) {
//                System.err.println("after fragmentOffset = " + fragmentOffset + ", receivedRaw = " + new String(receivedRaw, dataOff, dataLen, StandardCharsets.UTF_8) + ", dataOff = " + dataOff + ", dataLen = " + dataLen);
//                    System.err.println("recv buffer before(" + firstValidIndexInBuffer + ", " + available + ") = \"" + new String(buffer, 0, 200, StandardCharsets.UTF_8) + "\"");
                available = (int) (underlyingFragmentStream.getEarliestMissingIndex() - firstIndexInBufferRepresents) - firstValidIndexInBuffer; //HAS TO BE CACHED(i.e. cannot be calculated on the fly) - otherwise nasty synchronization problems arise... (would have to synchronize in this class over underlyingFragment stream moinitor instance

                int bufferOffset = (int) (fragmentOffset - firstIndexInBufferRepresents);
//                    System.err.println("dataOff1 = " + dataOff);
//                    System.err.println("bufferOffset1 = " + bufferOffset);
//                    System.err.println("dataLen1 = " + dataLen);
                if (bufferOffset < 0) {//if the fragment is already partially known
                    dataLen -= -bufferOffset;
                    dataOff += -bufferOffset;
                    bufferOffset = 0;
                }
//                    System.err.println("recv fragmentOffset = " + fragmentOffset);
//                    System.err.println("firstIndexInBufferRepresents = " + firstIndexInBufferRepresents);
//                    System.err.println("firstValidIndexInBuffer = " + firstValidIndexInBuffer);
//                    System.err.println("dataOff2 = " + dataOff);
//                    System.err.println("bufferOffset2 = " + bufferOffset);
//                    System.err.println("dataLen2 = " + dataLen);
//                    System.err.println("buffer.length = " + buffer.length);
//                    System.err.println("recv available = " + available);
                if (dataOff < receivedRaw.length && dataLen > 0) {
                    int toCopy = Math.min(receivedRaw.length - dataOff, dataLen);
//                        System.err.println("in received(asString): " + new String(receivedRaw, dataOff, toCopy, StandardCharsets.UTF_8));


//                    todo -if len exceeds buffer cap - ask output stream to slow down if data is not being read...

                    if (buffer.length < bufferOffset + toCopy) {
//                        System.err.println("grow");
//                        System.err.println("grow - buffer 1 ("+buffer.length+")= " /*+ Arrays.toString(buffer)*/);
//                        System.err.println("grow - bufferOffset = " + bufferOffset);
//                        System.err.println("grow - toCopy = " + toCopy);
//                        System.err.println("grow - firstValidIndexInBuffer = " + firstValidIndexInBuffer);
//                        System.err.println("grow - firstIndexInBufferRepresents = " + firstIndexInBufferRepresents);
                        System.arraycopy(buffer, firstValidIndexInBuffer, buffer, 0, buffer.length - firstValidIndexInBuffer);
//                        System.err.println("grow - buffer 2 ("+buffer.length+")= " /*+ Arrays.toString(buffer)*/);
                        firstIndexInBufferRepresents += firstValidIndexInBuffer;
//                        bufferOffset -= firstValidIndexInBuffer;
                        firstValidIndexInBuffer = 0;

                        bufferOffset = (int) (fragmentOffset - firstIndexInBufferRepresents);

//                        System.err.println("grow - firstValidIndexInBuffer = " + firstValidIndexInBuffer);
//                        System.err.println("grow - firstIndexInBufferRepresents = " + firstIndexInBufferRepresents);
//                        System.err.println("grow - bufferOffset = " + bufferOffset);


                        if (buffer.length <= bufferOffset + toCopy)
                            buffer = ByteArrayStorage.grow_to_at_least(buffer, buffer.length, bufferOffset + toCopy, false);
//                        System.err.println("grow - buffer 3 ("+buffer.length+")= " /*+ Arrays.toString(buffer)*/);
                    }
                    System.arraycopy(receivedRaw, dataOff, buffer, bufferOffset, toCopy);
//                        System.err.println("recv buffer after(" + firstValidIndexInBuffer + ", " + available + ") = \"" + new String(buffer, 0, 200, StandardCharsets.UTF_8) + "\"");
                }

//                if(buffer.contentSize() > MAX_BUFFER_SIZE)
//                    System.err.println("echelon etc");

                notify();
//                SyncHelp.notify(this);
            }

//            AverageCallTimeMarker.mark_call_end("receiver");
        });
    }

    @Override public synchronized int read(int timeout_ms) throws IOException {
        if(! SyncHelp.waitUntil(this, () -> available>0 || underlyingFragmentStream.isClosed(), timeout_ms))
            throw new IOException("timeout");
        if(underlyingFragmentStream.isClosed() && !underlyingFragmentStream.isFullyReceived())
            throw new IOException("input stream was closed using close");
        if(available == 0) return -1;

        int read = buffer[firstValidIndexInBuffer] & 0xFF;
        firstValidIndexInBuffer++;
        available--;
        return read;
    }

    @Override public synchronized int read(byte[] b, int off, int len, int timeout_ms) throws IOException {
        if(! SyncHelp.waitUntil(this, () -> available>0 || underlyingFragmentStream.isClosed(), timeout_ms))
            throw new IOException("timeout");
        if(underlyingFragmentStream.isClosed() && !underlyingFragmentStream.isFullyReceived())
            throw new IOException("input stream was closed using close");
        int numberOfBytesToCopy = Math.min(len, available);
//        System.err.println("read(avai="+available+") - "+(firstIndexInBufferRepresents+firstValidIndexInBuffer)+", "+(firstIndexInBufferRepresents+firstValidIndexInBuffer + numberOfBytesToCopy));
        if(available == 0) return -1;


//        if(numberOfBytesToCopy == 0 && underlyingFragmentStream.isClosed())
//            return -1;
        System.arraycopy(buffer, firstValidIndexInBuffer, b, off, numberOfBytesToCopy); //copy from buffer - duh

        firstValidIndexInBuffer += numberOfBytesToCopy;

        available -= numberOfBytesToCopy;

        return numberOfBytesToCopy;
    }


    @Override public int available() {
        return available;
    }
    @Override public void received(P2LMessage message) {
        underlyingFragmentStream.received(message);
    }
    @Override public void close() throws IOException {
        underlyingFragmentStream.close();
    }
    @Override public boolean isClosed() {
        return underlyingFragmentStream.isClosed();
    }
}
