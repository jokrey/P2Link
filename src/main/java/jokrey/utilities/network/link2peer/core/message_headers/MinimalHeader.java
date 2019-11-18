package jokrey.utilities.network.link2peer.core.message_headers;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;

import java.util.Objects;

/**
 * @author jokrey
 */
public class MinimalHeader implements P2LMessageHeader {
    private final P2Link sender;
    private final int type;
    private boolean requestReceipt;
    public MinimalHeader(P2Link sender, int type, boolean requestReceipt) {
        this.sender = sender;
        this.type = type;
        this.requestReceipt = requestReceipt;
    }

    @Override public P2Link getSender() {
        return sender;
    }

    @Override public int getType() {
        return type;
    }

    @Override public boolean requestReceipt() {
        return requestReceipt;
    }

    @Override public void mutateToRequestReceipt(byte[] raw) {
        raw[HEADER_OFFSET_FLAG_BYTE] = BitHelper.setBit(raw[HEADER_OFFSET_FLAG_BYTE], HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        requestReceipt = true;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MinimalHeader that = (MinimalHeader) o;
        return Objects.equals(sender, that.sender) && equalsIgnoreVolatile(that);
    }
    @Override public int hashCode() {
        return Objects.hash(sender, type, requestReceipt, isReceipt(), isStreamEof(), isLongPart(), isStreamPart(), getConversationId(), getExpiresAfter(), getPartIndex(), getNumberOfParts());
    }
    @Override public String toString() {
        return getClass().getSimpleName()+"{" + "sender='" + sender + '\'' + ", type=" + type + (isConversationIdPresent()?", conversationId=" + getConversationId():"") +
                (isExpirationPresent()?", expiresAfter=" + getExpiresAfter():"") +
                ", requestReceipt=" + requestReceipt + ", isReceipt=" + isReceipt() + '}';
    }

    @Override public boolean isExpired() { return true; }
    @Override public boolean isReceipt() { return false; }
    @Override public int getConversationId() { return P2LNode.NO_CONVERSATION_ID; }
    @Override public short getExpiresAfter() { return P2LMessage.EXPIRE_INSTANTLY; }
    @Override public boolean isLongPart() { return false; }
    @Override public boolean isStreamPart() { return false; }
    @Override public boolean isStreamEof() { return false; }
    @Override public int getPartIndex() { return 0; }
    @Override public int getNumberOfParts() { return 0; }
}
