package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class FullShortMessageHeader extends CustomExpirationHeader {
    private final short conversationId;
    public FullShortMessageHeader(P2Link sender, short type, short conversationId, short expiresAfter, boolean requestReceipt) {
        super(sender, type, expiresAfter, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public short getConversationId() {
        return conversationId;
    }
}
