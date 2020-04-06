package jokrey.utilities.network.link2peer.node.message_headers;

import java.net.InetSocketAddress;

/**
 * @author jokrey
 */
public class ConversationIdHeader extends MinimalHeader {
    private final short conversationId;
    public ConversationIdHeader(InetSocketAddress sender, short type, short conversationId, boolean requestReceipt) {
        super(sender, type, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public short getConversationId() {
        return conversationId;
    }
}
