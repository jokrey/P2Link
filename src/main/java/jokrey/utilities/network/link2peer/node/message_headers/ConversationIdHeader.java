package jokrey.utilities.network.link2peer.node.message_headers;

import java.net.InetSocketAddress;

/**
 * @author jokrey
 */
public class ConversationIdHeader extends MinimalHeader {
    private final short conversationId;
    public ConversationIdHeader(short type, short conversationId, boolean requestReceipt) {
        super(type, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public short getConversationId() {
        return conversationId;
    }
}
