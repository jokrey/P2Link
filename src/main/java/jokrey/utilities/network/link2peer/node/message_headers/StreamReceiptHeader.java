package jokrey.utilities.network.link2peer.node.message_headers;

/**
 * @author jokrey
 */
public class StreamReceiptHeader extends ConversationHeader {
    public StreamReceiptHeader(short type, short conversationId, short step, boolean eofIndicator) {
        super(type, conversationId, step, eofIndicator);
    }

    @Override public boolean isReceipt() { return true; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return requestReceipt(); }

    @Override public String toString() { return "StreamReceiptHeader{isStreamEof=" + isStreamEof() + '}'; }
}
