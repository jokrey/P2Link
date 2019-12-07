package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
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
public interface P2LInputStream extends AutoCloseable {
    /** Internally used to propagate appropriate raw messages to the stream */
    void received(P2LMessage message);

    /** Closes this stream - subsequent calls to most methods may now thrown exceptions. Has to be idempotent. */
    @Override void close() throws IOException;

    /**
     * @return whether the current stream was either closed using {@link #close()} or the stream was marked as closed(eof) by the sender and all bytes were read from it.
     */
    boolean isClosed();
}
