package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class ReceiptHeader extends ConversationHeader {
    public ReceiptHeader(P2Link sender, short type, short conversationId, short step) {
        this(sender, type, conversationId, step, false);
    }
    public ReceiptHeader(P2Link sender, short type, short conversationId, short step, boolean requestReceipt) {
        super(sender, type, conversationId, step, requestReceipt);
    }

    @Override public boolean isReceipt() {
        return true;
    }

    @Override public String toString() {
        return "ReceiptHeader{" +
                "type=" + getType() +
                ", conversationId=" + getConversationId() +
                ", step=" + getStep() +
                '}';
    }
}
