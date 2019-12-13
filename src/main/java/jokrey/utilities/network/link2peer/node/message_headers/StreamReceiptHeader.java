package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class StreamReceiptHeader extends ConversationIdHeader {
    public StreamReceiptHeader(P2Link sender, short type, short conversationId, boolean eofIndicator) {
        super(sender, type, conversationId, eofIndicator);

        //TODO - eofIndicator == REQUEST RECEIPT IS A WEIRD INTERNAL HACK THAT WILL WORK BUT IS A BIT WEIRD SO KEEP THAT IN MIND PLEASE
    }

    @Override public boolean isReceipt() { return true; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return requestReceipt(); }

    @Override public String toString() { return "StreamReceiptHeader{isStreamEof=" + isStreamEof() + '}'; }
}
