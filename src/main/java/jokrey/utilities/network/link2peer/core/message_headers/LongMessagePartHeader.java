package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class LongMessagePartHeader extends FullShortMessageHeader {
    private final int index, size;
    public LongMessagePartHeader(String sender, int type, int conversationId, short expiresAfter, int index, int size) {
        this(sender, type, conversationId, expiresAfter, index, size, false);
    }
    public LongMessagePartHeader(String sender, int type, int conversationId, short expiresAfter,
                                 int index, int size, boolean requestReceipt) {
        super(sender, type, conversationId, expiresAfter, requestReceipt);
        this.index = index;
        this.size = size;
    }

    @Override public int getPartIndex() { return index; }
    @Override public int getNumberOfParts() { return size; }
    @Override public boolean isLongPart() { return true; }
}
