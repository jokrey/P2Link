package jokrey.utilities.network.link2peer.node.message_headers;

/**
 * @author jokrey
 */
public class LongMessagePartHeader extends FullShortMessageHeader {
    private final int index, size;
    private final short step;
    public LongMessagePartHeader(short type, short conversationId, short expiresAfter, short step, int index, int size) {
        this(type, conversationId, expiresAfter, step, index, size, false);
    }
    public LongMessagePartHeader(short type, short conversationId, short expiresAfter, short step,
                                 int index, int size, boolean requestReceipt) {
        super(type, conversationId, expiresAfter, requestReceipt);
        this.index = index;
        this.size = size;
        this.step = step;
    }

    @Override public short getStep() { return step; }
    @Override public int getPartIndex() { return index; }
    @Override public int getNumberOfParts() { return size; }
    @Override public boolean isLongPart() { return true; }
}
