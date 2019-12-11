package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.network.link2peer.P2Link;

/**
 * @author jokrey
 */
public class FullShortMessageHeader extends CustomExpirationHeader {
    private final int conversationId;
    public FullShortMessageHeader(P2Link sender, int type, int conversationId, short expiresAfter, boolean requestReceipt) {
        super(sender, type, expiresAfter, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public int getConversationId() {
        return conversationId;
    }
}
