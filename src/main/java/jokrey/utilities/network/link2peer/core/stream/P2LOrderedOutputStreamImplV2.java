package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.debug_analysis_helper.TimeDiffMarker;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * @author jokrey
 */
public class P2LOrderedOutputStreamImplV2 extends P2LOrderedOutputStream implements P2LFragmentOutputStream.FragmentRetriever  {
    private final P2LFragmentOutputStreamImplV1 underlyingFragmentStream;
    private final TransparentBytesStorage buffer = new ByteArrayStorage(P2LOrderedInputStreamImplV2.MAX_BUFFER_SIZE);
    private long firstBufferIndexRepresents = 0;

    protected P2LOrderedOutputStreamImplV2(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId) {
        super(parent, to, con, type, conversationId);
        underlyingFragmentStream = new P2LFragmentOutputStreamImplV1(parent, to, con, type, conversationId);
        underlyingFragmentStream.setSource(this);
    }

    @Override public void write(int b) throws IOException {
        if(underlyingFragmentStream.isClosed()) throw new IOException("Stream closed");
        buffer.append(new byte[] {(byte) (b&0xFF)});
//        System.out.println("write1 - buffer.asString = " + new String(buffer.getContent(), (int) firstBufferIndexRepresents, (int) buffer.contentSize(), StandardCharsets.UTF_8));
    }

    @Override public void write(byte[] b, int off, int len) throws IOException {
        if(underlyingFragmentStream.isClosed()) throw new IOException("Stream closed");
        buffer.set(buffer.contentSize(), b, off, len);
        if(buffer.contentSize() > P2LOrderedInputStreamImplV2.MAX_BUFFER_SIZE/2)
            flush();
//        System.out.println("writeN - buffer.asString = " + new String(buffer.getContent(), (int) firstBufferIndexRepresents, (int) buffer.contentSize(), StandardCharsets.UTF_8));
    }

    @Override public void flush() throws IOException {
        if(underlyingFragmentStream.isClosed()) throw new IOException("Stream closed");
        underlyingFragmentStream.send();

//        System.out.println("flush - buffer.asString = " + new String(buffer.getContent(), (int) firstBufferIndexRepresents, (int) buffer.contentSize(), StandardCharsets.UTF_8));
    }



    @Override public SubBytesStorage sub(long start, long end) {
        return buffer.subStorage(start, end);
    }

    @Override public long currentMaxEnd() {
        return firstBufferIndexRepresents + buffer.contentSize();
    }

    @Override public long totalNumBytes() {
        return closedAt;
    }

    @Override public void adviceEarliestRequiredIndex(long index) {
        buffer.delete(0, index - firstBufferIndexRepresents);
        firstBufferIndexRepresents = index;
    }



    private long closedAt = -1;
    @Override public void receivedReceipt(P2LMessage rawReceipt) {
        underlyingFragmentStream.receivedReceipt(rawReceipt);
    }
    @Override public boolean waitForConfirmationOnAll(int timeout_ms) throws IOException {
        return underlyingFragmentStream.waitForConfirmationOnAll(timeout_ms);
    }
    @Override public boolean close(int timeout_ms) {
        TimeDiffMarker.setMark("P2LOrderedOutputStreamImplV2.close");
        try {
            closedAt = firstBufferIndexRepresents + buffer.contentSize();
            return underlyingFragmentStream.close(timeout_ms);
        } finally {
            TimeDiffMarker.println("P2LOrderedOutputStreamImplV2.close");
        }
    }
    @Override public boolean isClosed() {
        return underlyingFragmentStream.isClosed();
    }
}
