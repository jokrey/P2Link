package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.LongTupleList;
import jokrey.utilities.network.link2peer.util.SyncHelp;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * todo - ask output stream to slow down if data is not being read...
 *
 * @author jokrey
 */
public class P2LOrderedInputStreamImplV2 extends P2LOrderedInputStream {
    public static int MAX_BUFFER_SIZE = 65536*100;// 16384;

    private final P2LFragmentInputStreamImplV1 underlyingFragmentStream;

    private long firstIndexInBufferRepresents = 0;
    private int available = 0;
    private byte[] buffer = new byte[MAX_BUFFER_SIZE];

    protected P2LOrderedInputStreamImplV2(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId) {
        super(parent, to, type, conversationId);
        underlyingFragmentStream = new P2LFragmentInputStreamImplV1(parent, to, con, type, conversationId);
        underlyingFragmentStream.addFragmentReceivedListener((fragmentOffset, receivedRaw, dataOff, dataLen) -> {

            synchronized (this) {
//                System.err.println("recv buffer before("+available+") = \"" + new String(buffer, StandardCharsets.UTF_8)+"\"");
                available = (int) (underlyingFragmentStream.getEarliestMissingIndex()-firstIndexInBufferRepresents); //HAS TO BE CACHED(i.e. cannot be calculated on the fly) - otherwise nasty synchronization problems arise... (would have to synchronize in this class over underlyingFragment stream moinitor instance

                int bufferOffset = (int) (fragmentOffset - firstIndexInBufferRepresents);
//                System.err.println("dataOff1 = " + dataOff);
//                System.err.println("bufferOffset1 = " + bufferOffset);
//                System.err.println("dataLen1 = " + dataLen);
                if(bufferOffset < 0) {//if the fragment is already partially known
                    dataLen -= -bufferOffset;
                    dataOff += -bufferOffset;
                    bufferOffset = 0;
                }
//                System.err.println("fragmentOffset = " + fragmentOffset);
//                System.err.println("firstIndexInBufferRepresents = " + firstIndexInBufferRepresents);
//                System.err.println("dataOff2 = " + dataOff);
//                System.err.println("bufferOffset2 = " + bufferOffset);
//                System.err.println("dataLen2 = " + dataLen);
//                System.err.println("recv available = " + available);
                if(dataOff < receivedRaw.length && dataLen > 0) {
//                    System.err.println("in received(asString): " + new String(receivedRaw, dataOff, Math.min(receivedRaw.length - dataOff, dataLen), StandardCharsets.UTF_8));
//                    todo -if len exceeds buffer cap - ask output stream to slow down if data is not being read...
                    int toCopy = Math.min(receivedRaw.length - dataOff, dataLen);
                    if(buffer.length < bufferOffset + toCopy) {
                        buffer = ByteArrayStorage.grow_to_at_least(buffer, buffer.length, bufferOffset + toCopy, false);
                    }
                    System.arraycopy(receivedRaw, dataOff, buffer, bufferOffset, toCopy);
//                    System.err.println("recv buffer after("+available+") = \"" + new String(buffer, StandardCharsets.UTF_8)+"\"");
                }

//                if(buffer.contentSize() > MAX_BUFFER_SIZE)
//                    System.err.println("echelon etc");

                notify();
//                SyncHelp.notify(this);
            }

        });
    }

    @Override public synchronized int read(int timeout_ms) throws IOException {
        if(! SyncHelp.waitUntil(this, () -> available>0 || underlyingFragmentStream.isClosed(), timeout_ms))
            throw new IOException("timeout");
        if(underlyingFragmentStream.isClosed() && !underlyingFragmentStream.isFullyReceived())
            throw new IOException("input stream was closed using close");
        if(available == 0) return -1;

        int read = buffer[0] & 0xFF;
        System.arraycopy(buffer, 1, buffer, 0, buffer.length-1); //todo: a delayed delete, when new data was received - tho array copy is probably good enough and who cares - more important is the other thing with the thing - i forgot - wait: ok got it: buffer limitation and out wait
        firstIndexInBufferRepresents++;
        available--;
        return read;
    }

    @Override public synchronized int read(byte[] b, int off, int len, int timeout_ms) throws IOException {
        if(! SyncHelp.waitUntil(this, () -> available>0 || underlyingFragmentStream.isClosed(), timeout_ms))
            throw new IOException("timeout");
        if(underlyingFragmentStream.isClosed() && !underlyingFragmentStream.isFullyReceived())
            throw new IOException("input stream was closed using close");
        if(available == 0) return -1;

        int numberOfBytesToCopy = Math.min(len, available);
//        System.err.println("read - available before = " + available);
//        System.err.println("read - firstIndexInBufferRepresents = " + firstIndexInBufferRepresents);
//        System.err.println("read - numberOfBytesToCopy = " + numberOfBytesToCopy);

        if(numberOfBytesToCopy == 0 && underlyingFragmentStream.isClosed())
            return -1;
//        System.err.println("read - buffer before("+available+") = \"" + new String(buffer, StandardCharsets.UTF_8)+"\"");
        System.arraycopy(buffer, 0, b, off, numberOfBytesToCopy); //copy from buffer - duh
//        System.err.println("read = \"" + new String(b, off, numberOfBytesToCopy, StandardCharsets.UTF_8)+"\"");
        System.arraycopy(buffer, numberOfBytesToCopy, buffer, 0, buffer.length-numberOfBytesToCopy); //todo: a delayed delete, when new data was received - tho array copy is probably good enough and who cares - more important is the other thing with the thing - i forgot - wait: ok got it: buffer limitation and out wait
        firstIndexInBufferRepresents += numberOfBytesToCopy;
        available -= numberOfBytesToCopy;
//        System.err.println("read - available after = " + available);
//        System.err.println("read - buffer after("+available+") = \"" + new String(buffer, StandardCharsets.UTF_8)+"\"");
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
