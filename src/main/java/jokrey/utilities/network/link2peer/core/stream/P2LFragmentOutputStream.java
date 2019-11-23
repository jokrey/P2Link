package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * @author jokrey
 */
public abstract class P2LFragmentOutputStream implements P2LOutputStream {
    protected final P2LNodeInternal parent;
    protected final SocketAddress to;
    protected final P2LConnection con;
    protected final int type;
    protected final int conversationId;
    protected final TransparentBytesStorage source;
    protected P2LFragmentOutputStream(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId, TransparentBytesStorage source) {
        this.parent = parent;
        this.to = to;
        this.con = con;
        this.type = type;
        this.conversationId = conversationId;
        this.source = source;
    }

    public void sendSource() throws InterruptedException, IOException {
        sendSource(0);
    }
    public abstract void sendSource(int timeout) throws InterruptedException, IOException;
}
