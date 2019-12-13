package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class ConversationIdHeader extends MinimalHeader {
    private final short conversationId;
    public ConversationIdHeader(P2Link sender, short type, short conversationId, boolean requestReceipt) {
        super(sender, type, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public short getConversationId() {
        return conversationId;
    }
}
