package jokrey.utilities.network.link2peer.node.message_headers;

import java.net.InetSocketAddress;

/**
 * @author jokrey
 */
public class LongMessagePartHeader extends FullShortMessageHeader {
    private final int index, size;
    private final short step;
    public LongMessagePartHeader(InetSocketAddress sender, short type, short conversationId, short expiresAfter, short step, int index, int size) {
        this(sender, type, conversationId, expiresAfter, step, index, size, false);
    }
    public LongMessagePartHeader(InetSocketAddress sender, short type, short conversationId, short expiresAfter, short step,
                                 int index, int size, boolean requestReceipt) {
        super(sender, type, conversationId, expiresAfter, requestReceipt);
        this.index = index;
        this.size = size;
        this.step = step;
    }

    @Override public short getStep() { return step; }
    @Override public int getPartIndex() { return index; }
    @Override public int getNumberOfParts() { return size; }
    @Override public boolean isLongPart() { return true; }
}
