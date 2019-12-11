package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class ReceiptHeader extends ConversationHeader {
    public ReceiptHeader(P2Link sender, int type, int conversationId) {
        this(sender, type, conversationId, false);
    }
    public ReceiptHeader(P2Link sender, int type, int conversationId, boolean requestReceipt) {
        super(sender, type, conversationId, requestReceipt);
    }

    @Override public boolean isReceipt() {
        return true;
    }
}
