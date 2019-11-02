package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class FullShortMessageHeader extends CustomExpirationHeader {
    private final int conversationId;
    public FullShortMessageHeader(String sender, int type, int conversationId, short expiresAfter, boolean requestReceipt) {
        super(sender, type, expiresAfter, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public int getConversationId() {
        return conversationId;
    }
}
