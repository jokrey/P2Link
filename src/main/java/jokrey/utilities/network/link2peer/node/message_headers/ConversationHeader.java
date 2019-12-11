package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class ConversationHeader extends MinimalHeader {
    private final int conversationId;
    public ConversationHeader(P2Link sender, int type, int conversationId, boolean requestReceipt) {
        super(sender, type, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public int getConversationId() {
        return conversationId;
    }
}
