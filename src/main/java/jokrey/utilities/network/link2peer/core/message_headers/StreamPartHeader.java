package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class StreamPartHeader extends ConversationHeader {
    public int index;
    private final boolean eofIndicator;
    public StreamPartHeader(String sender, int type, int conversationId, int index, boolean requestReceipt, boolean eofIndicator) {
        super(sender, type, conversationId, requestReceipt);

        this.index = index;
        this.eofIndicator = eofIndicator;
    }

    @Override public int getPartIndex() { return index; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return eofIndicator; }

    @Override public String toString() { return "StreamPartHeader{" + "index=" + index + ", requestReceipt=" + requestReceipt() + ", isStreamEof=" + isStreamEof() + '}'; }
}
