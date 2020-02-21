package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class ConversationHeader extends MinimalHeader {
    private final short conversationId;
    public short step;
    public ConversationHeader(P2Link sender, short type, short conversationId, short step, boolean requestReceipt) {
        super(sender, type, requestReceipt);
        this.conversationId = conversationId;
        this.step = step;
    }

    @Override public short getConversationId() {
        return conversationId;
    }
    @Override public short getStep() {
        return step;
    }

    @Override public String toString() {
        return "ConversationHeader{" +
                "type=" + getType() +
                ", conversationId=" + conversationId +
                ", step=" + step +
                ", requestReceipt=" + requestReceipt() +
                '}';
    }
}
