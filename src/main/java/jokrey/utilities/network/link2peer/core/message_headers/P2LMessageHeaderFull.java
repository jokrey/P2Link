package jokrey.utilities.network.link2peer.core.message_headers;

import jokrey.utilities.bitsandbytes.BitHelper;

import java.util.Objects;

/**
 * Consider for protocol comparison with udp(8 bytes header) and tcp(20 bytes header min):
 * Header size is +8, because of underlying udp protocol
 *
 * Max header size(for broken up messages):
 *    1(flags) + 4(type) + 4(convId, optional) + 2(expiration, optional) + 8(long msg, index+size) + 8(udp) = 19 + 8 = 27 byte (on top of ip)
 *
 * TODO - multiple header classes ('SimpleHeader' does not require storage of conversation id and expiration - its getter can statically return the values - should safe on object size)
 *
 * @author jokrey
 */
public class P2LMessageHeaderFull implements P2LMessageHeader {
    private final String sender;
    @Override public String getSender() { return sender; }

    private final int type;
    @Override public int getType() { return type; }
    private final int conversationId;
    @Override public int getConversationId() { return conversationId; }

    //cannot be merged, index 0 is not necessarily the first packet received
    private final int partIndex;
    @Override public int getPartIndex() { return partIndex; }
    private final int partNumberOfParts;
    @Override public int getNumberOfParts() { return partNumberOfParts; }

    private boolean requestReceipt = false;
    @Override public boolean requestReceipt() {
        return requestReceipt;
    }
    private final boolean isReceipt;
    @Override public boolean isReceipt() {
        return isReceipt;
    }
    private final boolean isLongPart;
    @Override public boolean isLongPart() { return isLongPart; }
    private final boolean isStreamPart;
    @Override public boolean isStreamPart() { return isStreamPart; }
    private final boolean isStreamEof;
    @Override public boolean isStreamEof() { return isStreamEof; }

    /**
     * The time in seconds until this message is removed from the message queues
     * For value <= 0 the message will never be added to the message queue, it is only considered if a consumer is waiting when it arrives.
     */
    public final short expiresAfter;
    @Override public short getExpiresAfter() {
        return expiresAfter;
    }

    private final long createdAtCtm;//automatically set in the constructor
    @Override public boolean isExpired() {
        return createdAtCtm>0 && (expiresAfter <= 0 || (System.currentTimeMillis() - createdAtCtm)/1e3 > expiresAfter);
    }



    public P2LMessageHeaderFull(String sender,
                                int type, int conversationId, short expiresAfter,
                                int partIndex, int partNumberOfParts,
                                boolean isReceipt, boolean isLongPart, boolean isStreamPart, boolean isStreamEof) {
        this.sender = sender;
        this.type = type;
        this.conversationId = conversationId;
        this.expiresAfter = expiresAfter;
        this.partIndex = partIndex;
        this.partNumberOfParts = partNumberOfParts;
        this.isReceipt = isReceipt;
        this.isLongPart = isLongPart;
        this.isStreamPart = isStreamPart;
        this.isStreamEof = isStreamEof;
        createdAtCtm = System.currentTimeMillis();
    }



    @Override public void mutateToRequestReceipt(byte[] raw) {
        raw[HEADER_OFFSET_FLAG_BYTE] = BitHelper.setBit(raw[HEADER_OFFSET_FLAG_BYTE], HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        requestReceipt = true;
    }




    @Override public String toString() {
        return "P2LMessageHeaderFull{sender='" + sender + '\'' +
                ", type=" + type + ", conversationId=" + conversationId + ", partIndex=" + partIndex + ", partNumberOfParts=" + partNumberOfParts +
                ", requestReceipt=" + requestReceipt + ", isReceipt=" + isReceipt + ", isLongPart=" + isLongPart + ", isStreamPart=" + isStreamPart +
                ", isStreamEof=" + isStreamEof +
                ", expiresAfter=" + expiresAfter + ", createdAtCtm=" + createdAtCtm + '}';
    }
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LMessageHeaderFull that = (P2LMessageHeaderFull) o;
        return Objects.equals(sender, that.sender) && createdAtCtm == that.createdAtCtm && equalsIgnoreVolatile(that);
    }
    @Override public int hashCode() {
        return Objects.hash(sender, type, conversationId, partIndex,  partNumberOfParts, requestReceipt, isReceipt, isLongPart, expiresAfter, createdAtCtm);
    }
}
