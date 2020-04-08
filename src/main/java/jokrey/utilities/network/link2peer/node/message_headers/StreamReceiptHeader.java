package jokrey.utilities.network.link2peer.node.message_headers;

import java.net.InetSocketAddress;

/**
 * @author jokrey
 */
public class StreamReceiptHeader extends ConversationHeader {
    public StreamReceiptHeader(short type, short conversationId, short step, boolean eofIndicator) {
        super(type, conversationId, step, eofIndicator);

        //TODO - eofIndicator == REQUEST RECEIPT IS A WEIRD INTERNAL HACK THAT WILL WORK BUT IS A BIT WEIRD SO KEEP THAT IN MIND PLEASE
    }

    @Override public boolean isReceipt() { return true; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return requestReceipt(); }

    @Override public String toString() { return "StreamReceiptHeader{isStreamEof=" + isStreamEof() + '}'; }
}
