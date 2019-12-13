package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class StreamPartHeader extends ConversationIdHeader {
    public int index;
    private final boolean eofIndicator;
    public StreamPartHeader(P2Link sender, short type, short conversationId, int index, boolean requestReceipt, boolean eofIndicator) {
        super(sender, type, conversationId, requestReceipt);

        this.index = index;
        this.eofIndicator = eofIndicator;
    }

    @Override public int getPartIndex() { return index; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return eofIndicator; }

    @Override public String toString() { return "StreamPartHeader{" + "index=" + index + ", requestReceipt=" + requestReceipt() + ", isStreamEof=" + isStreamEof() + '}'; }
}
