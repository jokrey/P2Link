package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * @author jokrey
 */
public class P2LOrderedOutputStreamImplV2 extends P2LOrderedOutputStream implements P2LFragmentOutputStream.FragmentRetriever  {
    private final P2LFragmentOutputStreamImplV1 underlyingFragmentStream;
    private final TransparentBytesStorage buffer = new ByteArrayStorage(P2LHeuristics.ORDERED_STREAM_V2_MAX_BUFFER_SIZE);
    private long firstBufferIndexRepresents = 0;

    protected P2LOrderedOutputStreamImplV2(P2LNodeInternal parent, SocketAddress to, P2LConnection con, short type, short conversationId, short step) {
        super(parent, to, con, type, conversationId, step);
        underlyingFragmentStream = new P2LFragmentOutputStreamImplV1(parent, to, con, type, conversationId, step);
        underlyingFragmentStream.setSource(this);
    }

    @Override public void write(int b) throws IOException {
        if(underlyingFragmentStream.isClosed()) throw new IOException("Stream closed");
        buffer.append(new byte[] {(byte) (b&0xFF)});
        if(buffer.contentSize() > P2LHeuristics.ORDERED_STREAM_V2_MAX_BUFFER_SIZE/2)
            flush();
//        System.out.println("write1 - buffer.asString = " + new String(buffer.getContent(), (int) firstBufferIndexRepresents, (int) buffer.contentSize(), StandardCharsets.UTF_8));
    }

    @Override public void write(byte[] b, int off, int len) throws IOException {
        if(underlyingFragmentStream.isClosed()) throw new IOException("Stream closed");
        buffer.set(buffer.contentSize(), b, off, len);
        if(buffer.contentSize() > P2LHeuristics.ORDERED_STREAM_V2_MAX_BUFFER_SIZE/2)
            flush();
//        System.out.println("writeN - buffer.asString = " + new String(buffer.getContent(), (int) firstBufferIndexRepresents, (int) buffer.contentSize(), StandardCharsets.UTF_8));
    }

    @Override public void flush() throws IOException {
        if(underlyingFragmentStream.isClosed()) throw new IOException("Stream closed");
        underlyingFragmentStream.send();

//        System.out.println("flush - buffer.asString = " + new String(buffer.getContent(), (int) firstBufferIndexRepresents, (int) buffer.contentSize(), StandardCharsets.UTF_8));
    }


    @Override public byte[] sub(P2LFragmentOutputStream.Fragment fragment) {
        return buffer.sub(fragment.realStartIndex - firstBufferIndexRepresents, fragment.realEndIndex - firstBufferIndexRepresents);
    }
    @Override public P2LFragmentOutputStream.Fragment sub(long start, long end) {
        return new P2LFragmentOutputStream.Fragment(this, start, end);
    }
    @Override public long currentMaxEnd() {
        return firstBufferIndexRepresents + buffer.contentSize();
    }
    @Override public long totalNumBytes() {
        return closedAt;
    }
    @Override public void adviceEarliestRequiredIndex(long index) {
//        TimeDiffMarker.setMark("adviceEarliestRequiredIndex");
//        System.out.println("adviceEarliestRequiredIndex - b - buffer.contentSize() = " + buffer.contentSize());
//        System.out.println("adviceEarliestRequiredIndex - b - index = " + index);
////        System.out.println("adviceEarliestRequiredIndex - b - buffer = " + buffer);
//        System.out.println("adviceEarliestRequiredIndex - b - firstBufferIndexRepresents = " + firstBufferIndexRepresents);
//        System.out.println("adviceEarliestRequiredIndex - b - (index - firstBufferIndexRepresents > 0) = " + (index - firstBufferIndexRepresents > 0));
        if(index - firstBufferIndexRepresents > 0) {
            buffer.delete(0, Math.min(buffer.contentSize(), index - firstBufferIndexRepresents));
        }
        firstBufferIndexRepresents = index;
//        System.out.println("adviceEarliestRequiredIndex - a - buffer.contentSize() = " + buffer.contentSize());
//        System.out.println("adviceEarliestRequiredIndex - a - index = " + index);
////        System.out.println("adviceEarliestRequiredIndex - a - buffer = " + buffer);
//        System.out.println("adviceEarliestRequiredIndex - a - firstBufferIndexRepresents = " + firstBufferIndexRepresents);
//        TimeDiffMarker.println("adviceEarliestRequiredIndex");
    }



    private long closedAt = -1;
    @Override public void receivedReceipt(P2LMessage rawReceipt) {
        underlyingFragmentStream.receivedReceipt(rawReceipt);
    }
    @Override public boolean waitForConfirmationOnAll(int timeout_ms) {
        return underlyingFragmentStream.waitForConfirmationOnAll(timeout_ms);
    }
    @Override public boolean close(int timeout_ms) {
        closedAt = firstBufferIndexRepresents + buffer.contentSize();
        return underlyingFragmentStream.close(timeout_ms);
    }
    @Override public boolean isClosed() {
        return underlyingFragmentStream.isClosed();
    }
}
