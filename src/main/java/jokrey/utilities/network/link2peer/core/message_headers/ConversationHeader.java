package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class ConversationHeader extends MinimalHeader {
    private final int conversationId;
    public ConversationHeader(String sender, int type, int conversationId, boolean requestReceipt) {
        super(sender, type, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public int getConversationId() {
        return conversationId;
    }
}
