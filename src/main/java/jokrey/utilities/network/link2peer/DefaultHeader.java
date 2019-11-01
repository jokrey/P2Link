package jokrey.utilities.network.link2peer;

import jokrey.utilities.bitsandbytes.BitHelper;

import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author jokrey
 */
public class DefaultHeader implements P2LMessageHeader {
    private final String sender;
    private final int type;
    private final boolean requestReceipt;
    public DefaultHeader(String sender, int type, boolean requestReceipt) {
        this.sender = sender;
        this.type = type;
        this.requestReceipt = requestReceipt;
    }

    @Override public String getSender() {
        return sender;
    }

    @Override public int getType() {
        return type;
    }

    @Override public boolean requestReceipt() {
        return requestReceipt;
    }

    @Override public P2LMessageHeader mutateToRequestReceipt(byte[] raw) {
        if(requestReceipt()) return this;
        P2LMessageHeader newHeader = new DefaultHeader(sender, type, true);
        writeTo(raw);
        return newHeader;
    }

    @Override public int getSize() {
        return 5;
    }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultHeader that = (DefaultHeader) o;
        return type == that.type && requestReceipt == that.requestReceipt && Objects.equals(sender, that.sender);
    }
    @Override public int hashCode() {
        return Objects.hash(sender, type, requestReceipt);
    }
    @Override public String toString() {
        return "DefaultHeader{" + "sender='" + sender + '\'' + ", type=" + type + ", requestReceipt=" + requestReceipt + '}';
    }

    @Override public boolean isExpired() { return true; }
    @Override public boolean isReceipt() { return false; }
    @Override public int getConversationId() { return P2LNode.NO_CONVERSATION_ID; }
    @Override public short getExpirationTimeoutInSeconds() { return P2LMessage.EXPIRE_INSTANTLY; }
    @Override public boolean isLongPart() { return false; }
    @Override public boolean isStreamPart() { return false; }
    @Override public boolean isFinalStreamPart() { return false; }
    @Override public int getPartIndex() { return 0; }
    @Override public int getNumberOfParts() { return 0; }
}
