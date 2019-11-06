package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.core.P2LNodeInternal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketAddress;

/**
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

    public abstract void write(int b) throws IOException;
    public abstract void write(byte[] b, int off, int len) throws IOException;

    public abstract void flush() throws IOException;

    public void close() throws IOException { close(0); }
    public abstract void close(int timeout) throws IOException;
    public abstract boolean isClosed();
    public abstract void waitForConfirmationOnAll(int timeout) throws IOException;
}
