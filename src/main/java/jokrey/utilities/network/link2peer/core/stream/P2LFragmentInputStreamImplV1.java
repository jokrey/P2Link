package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * @author jokrey
 */
public class P2LFragmentInputStreamImplV1 extends P2LFragmentInputStream {
    protected P2LFragmentInputStreamImplV1(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        super(parent, to, type, conversationId);
    }

    @Override public void received(P2LMessage message) {

    }

    @Override public void close() throws IOException {

    }

    @Override public boolean isClosed() {
        return false;
    }
}
