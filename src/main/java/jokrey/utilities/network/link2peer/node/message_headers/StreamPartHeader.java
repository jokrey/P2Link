package jokrey.utilities.network.link2peer.node.message_headers;

import java.net.InetSocketAddress;

/**
 * todo 64 bit offsets (MAYBE: HIGH PRECISION FLAG IN HEADER THAT IS ACTIVATED ON THE FLY ONLY IF REQUIRED)
 *
 * @author jokrey
 */
public class StreamPartHeader extends ConversationHeader {
    public int index;
    private final boolean eofIndicator;
    public StreamPartHeader(short type, short conversationId, short step, int index, boolean requestReceipt, boolean eofIndicator) {
        super(type, conversationId, step, requestReceipt);

        this.index = index;
        this.eofIndicator = eofIndicator;
    }

    @Override public int getPartIndex() { return index; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return eofIndicator; }

    @Override public String toString() { return "StreamPartHeader{" + "type=" + getType() + ", conversationId=" + getConversationId() + ", step=" + getStep() + ", index=" + index + ", requestReceipt=" + requestReceipt() + ", isStreamEof=" + isStreamEof() + '}'; }
}
