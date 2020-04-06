package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * An output stream's abstract definition which can be used to send streams of data to peers.
 *
 * The actual implementation may vary and improved upon, but this interface will remain steady for all of them.
 *
 * @author jokrey
 */
public abstract class P2LOrderedOutputStream extends OutputStream implements P2LOutputStream {
    protected final P2LNodeInternal parent;
    protected final InetSocketAddress to;
    protected final P2LConnection con;
    protected final short type, conversationId, step;

    protected P2LOrderedOutputStream(P2LNodeInternal parent, InetSocketAddress to, P2LConnection con, short type, short conversationId, short step) {
        this.parent = parent;
        this.to = to;
        this.con = con;
        this.type = type;
        this.conversationId = conversationId;
        this.step = step;
    }
    @Override public InetSocketAddress getRawFrom() { return to; }
    @Override public short getType() { return type; }
    @Override public short getConversationId() { return conversationId; }
    @Override public short getStep() { return step; }

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

    //MUST BE REIMPLEMENTED HERE; BECAUSE THE DEFAULT IMPLEMENTATION IS OVERRIDEN BY THE NO-OP DEFINITION IN OUTPUTSTREAM - THIS IS BULLSHIT
    public void close() throws IOException {
        close(0);
    }
}
