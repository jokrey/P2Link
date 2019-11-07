package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.core.P2LNodeInternal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;

/**
 * An output stream's abstract definition which can be used to send streams of data to peers.
 *
 * The actual implementation may vary and improved upon, but this interface will remain steady for all of them.
 *
 * @author jokrey
 */
public abstract class P2LOutputStream extends OutputStream implements AutoCloseable {
    protected final P2LNodeInternal parent;
    protected final SocketAddress to;
    protected final int type;
    protected final int conversationId;

    protected P2LOutputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.type = type;
        this.conversationId = conversationId;
    }

    /**
     * Writes a single byte of data. This method is non blocking.
     * When it returns no guarantees are made about the data having been received by the peer.
     * In fact no guarantee is made that even an attempt is made at sending the data.
     * To get guarantees on either of those use {@link #flush()} and {@link #waitForConfirmationOnAll(int)}.
     *
     * @param b byte to be send (within range 0 - 255)
     * @throws IOException if an attempt is made at sending the data and it fails
     */
    public abstract void write(int b) throws IOException;
    /**
     * Writes a subset of bytes from an array of bytes of data. This method is non blocking.
     * When it returns no guarantees are made about the data having been received by the peer.
     * In fact no guarantee is made that even an attempt is made at sending the data.
     * To get guarantees on either of those use {@link #flush()} and {@link #waitForConfirmationOnAll(int)}.
     *
     * @param b byte array to be send from.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an attempt is made at sending the data and it fails
     */
    public abstract void write(byte[] b, int off, int len) throws IOException;

    /**
     * Flushes the current buffer (filled using {@link #write(int) or one of the other write methods}).
     * This entails creating a P2LMessage from it and sending it to the peer.
     * When this method returns, no guarantees are made about whether the peer has received any of the data send, to get this guarantee use {@link #waitForConfirmationOnAll(int)}.
     * @throws IOException if creating the message or sending it fails.
     */
    public abstract void flush() throws IOException;

    /**
     * Blocking method to obtain the guarantee that all data written AND subsequently flushed has been received by the peer.
     * When the method returns all data has been correctly received by the peer.
     * However no guarantee can be made whether the client has read and interpreted the data.
     *
     * @param timeout_ms timeout after which to throw a timeout exception -  if the timeout is 0 the method will block potentially forever
     * @throws IOException if the underlying socket has an error
     * @return whether confirmation has been received within the given timeout
     */
    public abstract boolean waitForConfirmationOnAll(int timeout_ms) throws IOException;

    /**
     * Closes the stream and cleans internal data structures.
     * Before this is done however, close calls {@link #flush()} and waits until all send messages have been received using {@link #waitForConfirmationOnAll(int)}.
     * In other words this method is blocking, potentially forever.
     * @throws IOException if flush fails
     */
    public void close() throws IOException { close(0); }
    /**
     * Closes the stream and cleans internal data structures.
     * Before this is done however, close calls {@link #flush()} and waits until all send messages have been received using {@link #waitForConfirmationOnAll(int)}.
     * After the given timeout the internal data structures are cleaned even without confirmation.
     *
     * This method is idempotent, i.e. calling it multiple times will not yield different results or change the internal state again.
     *
     * @param timeout_ms timeout after which to force the stream to close - if the timeout is 0 the method will blocking until confirmation is received.
     * @return whether confirmation was received before the
     * @throws IOException if flush fails
     */
    public abstract boolean close(int timeout_ms) throws IOException;

    /**
     * @return whether {@link #close()} was ever called, or the receiving input stream informed us that they have closed the stream on their side.
     */
    public abstract boolean isClosed();
}
