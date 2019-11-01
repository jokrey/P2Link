package jokrey.utilities.network.link2peer;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.util.Hash;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static jokrey.utilities.network.link2peer.P2LMessage.EXPIRE_INSTANTLY;

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
public class P2LMessageHeader {
    /**
     * Sender of the message
     * for individual messages this will be the peer the message was received from (automatically determined from the udp package sender address)
     * for broadcast messages this will be the peer that originally began distributing the message
     */
    public final String sender;

    /** get the sender of the message as a socket address - uses the {@link WhoAmIProtocol} */
    public InetSocketAddress senderAsSocketAddress() {
        return WhoAmIProtocol.fromString(sender);
    }

    /**
     * Type of the message. A shortcut for applications to determine what this message represents without decoding the data field.
     * Also used to wait-for/expect certain types of messages and assigning custom handlers.
     */
    public final int type;
    /**
     * Conversation id
     * Unique id that should be created using {@link P2LNode#createUniqueConversationId()}, when a conversation is established.
     * All subsequent messages in the conversation should use the id.
     * When retrying a conversation after package loss or canceling a conversation, this id can be used to distinguish messages in the queues.
     */
    public final int conversationId;

    //cannot be merged, index 0 is not necessarily the first packet received
    public final int partIndex;
    public final int partNumberOfParts;
    
    /** Whether this message expects a receipt (will automatically be send by the node) */
    public final boolean requestReceipt;
    /** Whether this message is a receipt (node has a special queue for receipts and automatically validates the receipts correctness) */
    public final boolean isReceipt;
    /** Whether this message is long - i.e. broken up into multiple packets */
    public final boolean isLongPart;

    /**
     * The time in seconds until this message is removed from the message queues
     * For value <= 0 the message will never be added to the message queue, it is only considered if a consumer is waiting when it arrives.
     */
    public final short expirationTimeoutInSeconds;
    /**
     * Only relevant to received messages
     * @return whether this message has expired, based on the time at which this object has been created
     */
    public boolean isExpired() {
        return createdAtCtm>0 && (expirationTimeoutInSeconds <= 0 || (System.currentTimeMillis() - createdAtCtm)/1e3 > expirationTimeoutInSeconds);
    }
    private final long createdAtCtm;//automatically set in the constructor




    public P2LMessageHeader(String sender,
                            int type, int conversationId, short expirationTimeoutInSeconds,
                            int partIndex, int partNumberOfParts,
                            boolean requestReceipt, boolean isReceipt, boolean isLongPart) {
        this.sender = sender;
        this.type = type;
        this.conversationId = conversationId;
        this.expirationTimeoutInSeconds = expirationTimeoutInSeconds;
        this.partIndex = partIndex;
        this.partNumberOfParts = partNumberOfParts;
        this.requestReceipt = requestReceipt;
        this.isReceipt = isReceipt;
        this.isLongPart = isLongPart;
        createdAtCtm = System.currentTimeMillis();
    }

    public void writeTo(byte[] raw) {
        BitHelper.writeInt32(raw, HEADER_BYTES_OFFSET_TYPE, type);
        int conversationIdFieldOffset = getConversationIdFieldOffset();
        int expirationFieldOffset = getExpirationFieldOffset();
        int partIndexAndSizeFieldOffset = getLongPartIndexAndSizeFieldOffset();

        byte flagByte = 0;

        if(conversationIdFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT);
            BitHelper.writeInt32(raw, conversationIdFieldOffset, conversationId);
        }
        if(expirationFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT);
            BitHelper.writeInt16(raw, expirationFieldOffset, expirationTimeoutInSeconds);
        }
        if(partIndexAndSizeFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART);
            BitHelper.writeInt32(raw, partIndexAndSizeFieldOffset, partIndex);
            BitHelper.writeInt32(raw, partIndexAndSizeFieldOffset+4, partNumberOfParts);
        }
        if(isReceipt) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT);
        if(requestReceipt) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        if(isLongPart) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART);
        raw[HEADER_OFFSET_FLAG_BYTE] = flagByte;
    }
    public static P2LMessageHeader from(byte[] raw, SocketAddress from) {
        int type = readTypeFromHeader(raw);
        byte flagByte = raw[HEADER_OFFSET_FLAG_BYTE];

        boolean conversationIdFieldPresent = readConversationIdPresentFromHeader(flagByte);
        boolean expirationFieldPresent = readExpirationPresentFromHeader(flagByte);
        boolean isLongPart = readIsLongPartFromHeader(flagByte);
        boolean requestReceipt = readRequestReceiptFromHeader(flagByte);
        boolean isReceipt = readIsReceiptFromHeader(flagByte);

        int conversationId = readConversationIdFromHeader(raw, conversationIdFieldPresent);
        short expirationTimeoutInSeconds = readExpirationFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent);
        int partIndex = readPartIndexFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, isLongPart);
        int partNumberOfParts = readPartSizeFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, isLongPart);

        return new P2LMessageHeader(WhoAmIProtocol.toString(from), type, conversationId, expirationTimeoutInSeconds, partIndex, partNumberOfParts, requestReceipt, isReceipt, isLongPart);
    }


    public P2LMessageHeader mutateToRequestReceipt(byte[] raw) {
        raw[HEADER_OFFSET_FLAG_BYTE] = BitHelper.setBit(raw[HEADER_OFFSET_FLAG_BYTE], HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        return new P2LMessageHeader(sender, type, conversationId, expirationTimeoutInSeconds, partIndex, partNumberOfParts, true, isReceipt, isLongPart);
    }


    public int getSize() {
        return getSize(isConversationIdPresent(), isExpirationPresent(), isLongPart);
    }
    public static int getSize(boolean isConversationIdPresent, boolean isExpirationPresent, boolean isLongPart) {
        int size = 5;//flag byte + type bytes
        if(isConversationIdPresent) size += 4;
        if(isExpirationPresent) size += 2;
        if(isLongPart) size += 8;
        return size;
    }



    private static final int HEADER_OFFSET_FLAG_BYTE = 0;
    private static final int HEADER_BYTES_OFFSET_TYPE = 1;
    public boolean isConversationIdPresent() {
        return conversationId != P2LNode.NO_CONVERSATION_ID;
    }
    private int getConversationIdFieldOffset() {
        return getConversationIdFieldOffset(isConversationIdPresent());
    }
    private static int getConversationIdFieldOffset(boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return -1;
        return 5;//from 5 to 9
    }
    public boolean isExpirationPresent() {
        return expirationTimeoutInSeconds != EXPIRE_INSTANTLY;
    }
    private int getExpirationFieldOffset() {
        return getExpirationFieldOffset(isConversationIdPresent(), isExpirationPresent());
    }
    private static int getExpirationFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return -1;
        if(conversationIdFieldPresent) return 9;//from 9 to 11
        return 5;//from 5 to 7
    }
    private int getLongPartIndexAndSizeFieldOffset() {
        return getLongPartIndexAndSizeFieldOffset(isConversationIdPresent(), isExpirationPresent(), isLongPart);
    }
    private static int getLongPartIndexAndSizeFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean isLongPart) {
        if(!isLongPart) return -1;
        if(conversationIdFieldPresent && expirationFieldPresent) return 11; //5+4+2=11, from 11 to 19
        if(conversationIdFieldPresent) return 9; //from 9 to 17
        if(expirationFieldPresent) return 7; //from 7 to 15
        return 5; //from 5 to 13
    }

    private static int readTypeFromHeader(byte[] raw) {
        return BitHelper.getInt32From(raw, HEADER_BYTES_OFFSET_TYPE);
    }
    private static int readConversationIdFromHeader(byte[] raw, boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return P2LNode.NO_CONVERSATION_ID;
        return BitHelper.getInt32From(raw, getConversationIdFieldOffset(conversationIdFieldPresent));
    }
    private static short readExpirationFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return EXPIRE_INSTANTLY;
        return BitHelper.getInt16From(raw, getExpirationFieldOffset(conversationIdFieldPresent, expirationFieldPresent));
    }
    private static int readPartIndexFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean isLongPart) {
        if(!isLongPart) return 0;
        return BitHelper.getInt32From(raw, getLongPartIndexAndSizeFieldOffset(conversationIdFieldPresent, expirationFieldPresent, isLongPart));
    }
    private static int readPartSizeFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean isLongPart) {
        if(!isLongPart) return 0;
        return BitHelper.getInt32From(raw, getLongPartIndexAndSizeFieldOffset(conversationIdFieldPresent, expirationFieldPresent, isLongPart)+4);
    }


    private static final int HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT = 0;
    private static final int HEADER_FLAG_BIT_OFFSET_IS_RECEIPT = 1;
    private static final int HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT = 2;
    private static final int HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT = 3;
    private static final int HEADER_FLAG_BIT_OFFSET_IS_LONG_PART = 4;
    private static final int HEADER_FLAG_BIT_OFFSET_X = 5;
    private static final int HEADER_FLAG_BIT_OFFSET_Y = 6;
    private static final int HEADER_FLAG_BIT_OFFSET_Z = 7;

    private static boolean readConversationIdPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT) == 1;
    }
    private static boolean readExpirationPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT) == 1;
    }
    private static boolean readIsLongPartFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART) == 1;
    }
    private static boolean readRequestReceiptFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT) == 1;
    }
    private static boolean readIsReceiptFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT) == 1;
    }

    public Hash contentHashFromIgnoreSender(byte[] raw, int payloadLength) {
        return contentHashFrom(null, raw, payloadLength);
    }
    public Hash contentHashFrom(byte[] raw, int payloadLength) {
        return contentHashFrom(sender, raw, payloadLength);
    }
    private Hash contentHashFrom(String sender, byte[] raw, int payloadLength) {
        if(isLongPart) throw new UnsupportedOperationException("content hash not supported for long messages");
        //fixme speed ok?
        try {
            MessageDigest hashFunction = MessageDigest.getInstance("SHA-1");
            if(sender!=null)
                hashFunction.update(sender.getBytes(StandardCharsets.UTF_8));
            hashFunction.update(raw, HEADER_BYTES_OFFSET_TYPE, 4); //type
            if(isConversationIdPresent())
                hashFunction.update(raw, getConversationIdFieldOffset(), 4); //conversation id
            if(payloadLength > 0)
                hashFunction.update(raw, getSize(), payloadLength); //only payload
            return new Hash(hashFunction.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new Error("missing critical algorithm");
        }
    }


    @Override public String toString() {
        return "P2LMessageHeader{sender='" + sender + '\'' +
                ", type=" + type + ", conversationId=" + conversationId + ", partIndex=" + partIndex + ", partNumberOfParts=" + partNumberOfParts +
                ", requestReceipt=" + requestReceipt + ", isReceipt=" + isReceipt + ", isLongPart=" + isLongPart +
                ", expirationTimeoutInSeconds=" + expirationTimeoutInSeconds + ", createdAtCtm=" + createdAtCtm + '}';
    }
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LMessageHeader that = (P2LMessageHeader) o;
        return Objects.equals(sender, that.sender) && createdAtCtm == that.createdAtCtm && equalsIgnoreVolatile(that);
    }
    public boolean equalsIgnoreVolatile(P2LMessageHeader that) {
        return  type == that.type && conversationId == that.conversationId &&
                partIndex == that.partIndex && partNumberOfParts == that.partNumberOfParts &&
                requestReceipt == that.requestReceipt && isReceipt == that.isReceipt && isLongPart == that.isLongPart &&
                expirationTimeoutInSeconds == that.expirationTimeoutInSeconds;
    }
    @Override public int hashCode() {
        return Objects.hash(sender, type, conversationId, partIndex,  partNumberOfParts, requestReceipt, isReceipt, isLongPart, expirationTimeoutInSeconds, createdAtCtm);
    }
}
