package jokrey.utilities.network.link2peer.node.message_headers;

/**
 * @author jokrey
 */
public class FullShortMessageHeader extends CustomExpirationHeader {
    private final short conversationId;
    public FullShortMessageHeader(short type, short conversationId, short expiresAfter, boolean requestReceipt) {
        super(type, expiresAfter, requestReceipt);
        this.conversationId = conversationId;
    }

    @Override public short getConversationId() {
        return conversationId;
    }
}
