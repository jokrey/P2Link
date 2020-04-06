package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * @author jokrey
 */
public class MinimalHeader implements P2LMessageHeader {
    private final InetSocketAddress sender;
    private final short type;
    public boolean requestReceipt;
    public MinimalHeader(InetSocketAddress sender, short type, boolean requestReceipt) {
        this.sender = sender;
        this.type = type;
        this.requestReceipt = requestReceipt;
    }

    @Override public InetSocketAddress getSender() {
        return sender;
    }

    @Override public short getType() {
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
                (isStepPresent()?", step=" + getStep():"") +
                (isLongPart()?", l-index=" + getPartIndex()+", l-size=" + getNumberOfParts():"") +
                ", requestReceipt=" + requestReceipt + ", isReceipt=" + isReceipt() + '}';
    }

    @Override public boolean isExpired() { return true; }
    @Override public boolean isReceipt() { return false; }
    @Override public short getConversationId() { return NO_CONVERSATION_ID; }
    @Override public short getStep() { return NO_STEP; }
    @Override public short getExpiresAfter() { return P2LMessage.EXPIRE_INSTANTLY; }
    @Override public boolean isLongPart() { return false; }
    @Override public boolean isStreamPart() { return false; }
    @Override public boolean isStreamEof() { return false; }
    @Override public int getPartIndex() { return 0; }
    @Override public int getNumberOfParts() { return 0; }
}
