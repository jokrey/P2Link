package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class ReceiptHeader extends ConversationHeader {
    public ReceiptHeader(String sender, int type, int conversationId) {
        this(sender, type, conversationId, false);
    }
    public ReceiptHeader(String sender, int type, int conversationId, boolean requestReceipt) {
        super(sender, type, conversationId, requestReceipt);
    }

    @Override public boolean isReceipt() {
        return true;
    }
}
