package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;

/**
 * @author jokrey
 */
public class P2LFragmentInputStreamImplV1 extends P2LFragmentInputStream {
    protected P2LFragmentInputStreamImplV1(P2LNodeInternal parent, SocketAddress to, int type, int conversationId, TransparentBytesStorage target) {
        super(parent, to, type, conversationId);
    }

    long highestEndReceived = 0;
    private boolean markReceived(long start, long end) {
        if(end > highestEndReceived) {
            highestEndReceived = end;
            return true;
        } else {
            return false; //todo -this is not true [get it?]
        }
    }

    @Override public void received(P2LMessage message) {
        long offsetOfMessage = message.header.getPartIndex();

        boolean wasMissing = markReceived(offsetOfMessage, offsetOfMessage + message.getPayloadLength());
        if(wasMissing) {
            try {
                parent.sendInternalMessage(P2LOrderedStreamReceipt.encode(type, conversationId, false, (int) highestEndReceived), to);
            } catch (IOException e) {
                e.printStackTrace();
            }

            fireReceived(message.header.getPartIndex(), message.content, message.header.getSize(), message.getPayloadLength());
        }
    }

    @Override public void close() throws IOException {

    }

    @Override public boolean isClosed() {
        return false;
    }
}
