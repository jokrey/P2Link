package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.core.P2LNodeInternal;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;

/**
 * An input stream's abstract definition which can be used to receive streams of data from peers.
 *
 * The actual implementation may vary and improved upon, but this interface will remain steady for all of them.
 *
 * @author jokrey
 */
public abstract class P2LOrderedInputStream extends InputStream implements P2LInputStream {
    protected final P2LNodeInternal parent;
    protected final SocketAddress to;
    protected final int type, conversationId;
    protected P2LOrderedInputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.type = type;
        this.conversationId = conversationId;
    }

    /**
     * Reads a single byte (represented as an integer between 0-255) from the stream.
     * If no data is available and will never become available, -1 is returned.
     * Blocking. If no data is available the method will block until data becomes available or the stream is closed.
     * @return the read byte or -1 if no data will ever become available
     * @throws IOException if some exception occurs.
     */
    @Override public int read() throws IOException {
        return read(0);
    }
    /**
     * Reads a number of bytes (represented as an integer between 0-255) from the stream and writes them into the given array.
     * If no data is available and will never become available, -1 is returned.
     * Blocking. If no data is available the method will block until data becomes available or the stream is closed.
     * @param b a byte array of min length = off + len
     * @param off the offset to start writing bytes into the given array
     * @param len maximum number of bytes to read and write
     * @return the read byte or -1 if no data will ever become available
     * @throws IOException if some exception occurs.
     */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        return read(b, off, len, 0);
    }

    /**
     * Like {@link #read()}.
     * If no byte is available after given timeout, a timeout exception is thrown.
     * This timeout exception does NOT indicate that no data will ever be read from the stream,
     *   it simply means that no data is or has become available within the given time frame
     * @param timeout_ms the timeout in milliseconds
     * @return a byte read from the stream or -1 if no byte is available and will ever become available
     * @throws IOException if the timeout is reached, the current thread is interrupted or the stream was closed
     */
    public abstract int read(int timeout_ms) throws IOException;
    /**
     * Like {@link #read(byte[], int, int)}.
     * If no byte is available after given timeout, a timeout exception is thrown.
     * This timeout exception does NOT indicate that no data will ever be read from the stream,
     *   it simply means that no data is or has become available within the given time frame
     * @param b a byte array of min length = off + len
     * @param off the offset to start writing bytes into the given array
     * @param len maximum number of bytes to read and write
     * @param timeout_ms the timeout in milliseconds
     * @return the actual number of bytes read from the stream or -1 if no bytes are available and will ever become available
     * @throws IOException if the timeout is reached, the current thread is interrupted or the stream was closed
     */
    public abstract int read(byte[] b, int off, int len, int timeout_ms) throws IOException;
}
