package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;

/**
 * @author jokrey
 */
public interface P2LOutputStream extends AutoCloseable {
    /** Internal use only. */
    void receivedReceipt(P2LMessage rawReceipt);

    /**
     * Blocking method to obtain the guarantee that all data written AND subsequently flushed has been received by the peer.
     * When the method returns all data has been correctly received by the peer.
     * However no guarantee can be made whether the client has read and interpreted the data.
     *
     * @param timeout_ms timeout after which to throw a timeout exception -  if the timeout is 0 the method will block potentially forever
     * @throws IOException if the underlying socket has an error
     * @return whether confirmation has been received within the given timeout
     */
     boolean waitForConfirmationOnAll(int timeout_ms) throws IOException;

    /**
     * Closes the stream and cleans internal data structures.
     * Before this is done however, close completes the current write(for example using flush) and waits until all send messages have been received using {@link #waitForConfirmationOnAll(int)}.
     * In other words this method is blocking, potentially forever.
     * @throws IOException if flush fails
     */
    default void close() throws IOException { close(0); }
    /**
     * Closes the stream and cleans internal data structures.
     * Before this is done however, close completes the current write(for example using flush) and waits until all send messages have been received using {@link #waitForConfirmationOnAll(int)}.
     * After the given timeout the internal data structures are cleaned even without confirmation.
     *
     * This method is idempotent, i.e. calling it multiple times will not yield different results or change the internal state again.
     *
     * @param timeout_ms timeout after which to force the stream to close - if the timeout is 0 the method will blocking until confirmation is received.
     * @return whether confirmation was received before the
     * @throws IOException if flush fails
     */
     boolean close(int timeout_ms) throws IOException;

    /**
     * @return whether {@link #close()} was ever called, or the receiving input stream informed us that they have closed the stream on their side.
     */
     boolean isClosed();
}
