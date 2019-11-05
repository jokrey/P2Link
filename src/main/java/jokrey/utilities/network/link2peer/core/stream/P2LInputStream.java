package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;

/** )
 *
 * @author jokrey
 */
public abstract class P2LInputStream extends InputStream implements AutoCloseable {
    protected final P2LNodeInternal parent;
    protected final SocketAddress to;
    protected final int type, conversationId;
    protected P2LInputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.type = type;
        this.conversationId = conversationId;
    }

    /** Internally used to propagate appropriate raw messages to the stream */
    abstract void received(P2LMessage message);

    @Override public int read() throws IOException {
        return read(0);
    }
    @Override public int read(byte[] b, int off, int len) throws IOException {
        return read(b, off, len, 0);
    }

    /**
     * Like {@link #read()}.
     * If no byte is available after given timeout, a timeout exception is thrown.
     * This timeout exception does NOT indicate that no data will ever be read from the stream,
     *   it simply means that no data is or has become available within the given timeframe
     * @param timeout_ms the timeout in milliseconds
     * @return a byte read from the stream or -1 if no byte is available and will ever become available
     * @throws IOException if the timeout is reached, the current thread is interrupted or the stream was closed
     */
    public abstract int read(int timeout_ms) throws IOException;
    /**
     * Like {@link #read(byte[], int, int)}.
     * If no byte is available after given timeout, a timeout exception is thrown.
     * This timeout exception does NOT indicate that no data will ever be read from the stream,
     *   it simply means that no data is or has become available within the given timeframe
     * @param timeout_ms the timeout in milliseconds
     * @return the number of bytes read from the stream or -1 if no bytes are available and will ever become available
     * @throws IOException if the timeout is reached, the current thread is interrupted or the stream was closed
     */
    public abstract int read(byte[] b, int off, int len, int timeout_ms) throws IOException;
    /**
     * @return whether the current stream was either closed using {@link #close()} or the stream was marked as closed(eof) by the sender and all bytes were read from it.
     */
    public abstract boolean isClosed();
}
