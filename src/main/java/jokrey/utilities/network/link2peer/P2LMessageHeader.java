package jokrey.utilities.network.link2peer;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.util.Hash;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
public interface P2LMessageHeader {
    /**
     * Sender of the message
     * for individual messages this will be the peer the message was received from (automatically determined from the udp package sender address)
     * for broadcast messages this will be the peer that originally began distributing the message
     */
    String getSender();

    /** get the sender of the message as a socket address - uses the {@link WhoAmIProtocol} */
    default InetSocketAddress getSenderAsSocketAddress() {
        return WhoAmIProtocol.fromString(getSender());
    }

    /**
     * Type of the message. A shortcut for applications to determine what this message represents without decoding the data field.
     * Also used to wait-for/expect certain types of messages and assigning custom handlers.
     */
    int getType();
    /**
     * Conversation id
     * Unique id that should be created using {@link P2LNode#createUniqueConversationId()}, when a conversation is established.
     * All subsequent messages in the conversation should use the id.
     * When retrying a conversation after package loss or canceling a conversation, this id can be used to distinguish messages in the queues.
     */
    int getConversationId();

    /**
     * The time in seconds until this message is removed from the message queues
     * For value <= 0 the message will never be added to the message queue, it is only considered if a consumer is waiting when it arrives.
     */
    short getExpirationTimeoutInSeconds();
    /**
     * Only relevant to received messages
     * @return whether this message has expired, based on the time at which this object has been created
     */
    boolean isExpired();
    
    /** Whether this message expects a receipt (will automatically be send by the node) */
    boolean requestReceipt();
    /** Whether this message is a receipt (node has a special queue for receipts and automatically validates the receipts correctness) */
    boolean isReceipt();
    /** Whether this message is long - i.e. broken up into multiple packets */
    boolean isLongPart();

    /** Whether this message is a message in the context of the stream protocol */
    boolean isStreamPart();
    boolean isFinalStreamPart();

    //cannot be merged, index 0 is not necessarily the first packet received
    int getPartIndex();
    int getNumberOfParts();





    P2LMessageHeader mutateToRequestReceipt(byte[] raw);

    default boolean equalsIgnoreVolatile(P2LMessageHeader that) {
        return  getType() == that.getType() && getConversationId() == that.getConversationId() &&
                getPartIndex() == that.getPartIndex() && getNumberOfParts() == that.getNumberOfParts() &&
                requestReceipt() == that.requestReceipt() && isReceipt() == that.isReceipt() && isLongPart() == that.isLongPart() &&
                getExpirationTimeoutInSeconds() == that.getExpirationTimeoutInSeconds();
    }
    default Hash contentHashFrom(byte[] raw, int payloadLength) {
        return contentHashFrom(getSender(), raw, payloadLength);
    }
    default Hash contentHashFromIgnoreSender(byte[] raw, int payloadLength) {
        return contentHashFrom(null, raw, payloadLength);
    }
    default Hash contentHashFrom(String sender, byte[] raw, int payloadLength) {
        //fixme speed ok?
        try {
            MessageDigest hashFunction = MessageDigest.getInstance("SHA-1");
            if(sender!=null)
                hashFunction.update(sender.getBytes(StandardCharsets.UTF_8));
            hashFunction.update(raw, HEADER_BYTES_OFFSET_TYPE, 4); //type
            if(getConversationId() != P2LNode.NO_CONVERSATION_ID)
                hashFunction.update(BitHelper.getBytes(getConversationId())); //conversation id
            if(payloadLength > 0)
                hashFunction.update(raw, getSize(), payloadLength); //only payload
            return new Hash(hashFunction.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new Error("missing critical algorithm");
        }
    }



//    void writeTo(byte[] raw);
//    static P2LMessageHeader from(byte[] raw, SocketAddress from) {
//
//    }












    default void writeTo(byte[] raw) {
        BitHelper.writeInt32(raw, HEADER_BYTES_OFFSET_TYPE, getType());
        int conversationIdFieldOffset = getConversationIdFieldOffset();
        int expirationFieldOffset = getExpirationFieldOffset();
        int partIndexAndSizeFieldOffset = getLongPartIndexAndSizeFieldOffset();

        byte flagByte = 0;

        if(conversationIdFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT);
            BitHelper.writeInt32(raw, conversationIdFieldOffset, getConversationId());
        }
        if(expirationFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT);
            BitHelper.writeInt16(raw, expirationFieldOffset, getExpirationTimeoutInSeconds());
        }
        if(partIndexAndSizeFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART);
            BitHelper.writeInt32(raw, partIndexAndSizeFieldOffset, getPartIndex());
            BitHelper.writeInt32(raw, partIndexAndSizeFieldOffset+4, getNumberOfParts());
        }
        if(isReceipt()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT);
        if(requestReceipt()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        if(isStreamPart()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART);
        raw[HEADER_OFFSET_FLAG_BYTE] = flagByte;
    }
    static P2LMessageHeader from(byte[] raw, SocketAddress from) {
        int type = readTypeFromHeader(raw);
        byte flagByte = raw[HEADER_OFFSET_FLAG_BYTE];

        boolean conversationIdFieldPresent = readConversationIdPresentFromHeader(flagByte);
        boolean expirationFieldPresent = readExpirationPresentFromHeader(flagByte);
        boolean isLongPart = readIsLongPartFromHeader(flagByte);
        boolean requestReceipt = readRequestReceiptFromHeader(flagByte);
        boolean isReceipt = readIsReceiptFromHeader(flagByte);
        boolean isStreamPart = readIsStreamPartFromHeader(flagByte);

        int conversationId = readConversationIdFromHeader(raw, conversationIdFieldPresent);
        short expirationTimeoutInSeconds = readExpirationFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent);
        int partIndex = readPartIndexFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, isLongPart);
        int partNumberOfParts = readPartSizeFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, isLongPart);

        String sender = WhoAmIProtocol.toString(from);

        return new P2LMessageHeaderFull(sender, type, conversationId, expirationTimeoutInSeconds, partIndex, partNumberOfParts, requestReceipt, isReceipt, isLongPart, isStreamPart);
    }
    static P2LMessageHeader from(String sender,
                                 int type, int conversationId, short expirationTimeoutInSeconds,
                                 int partIndex, int partNumberOfParts,
                                 boolean requestReceipt, boolean isReceipt, boolean isLongPart, boolean isStreamPart) {
        return new P2LMessageHeaderFull(sender, type, conversationId, expirationTimeoutInSeconds, partIndex, partNumberOfParts, requestReceipt, isReceipt, isLongPart, isStreamPart);
    }

    default int getSize() {
        return getSize(isConversationIdPresent(), isExpirationPresent(), isLongPart());
    }
    static int getSize(boolean isConversationIdPresent, boolean isExpirationPresent, boolean isLongPart) {
        int size = 5;//flag byte + type bytes
        if(isConversationIdPresent) size += 4;
        if(isExpirationPresent) size += 2;
        if(isLongPart) size += 8;
        return size;
    }
    default boolean isConversationIdPresent() {
        return getConversationId() != P2LNode.NO_CONVERSATION_ID;
    }
    default int getConversationIdFieldOffset() {
        return getConversationIdFieldOffset(isConversationIdPresent());
    }
    static int getConversationIdFieldOffset(boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return -1;
        return 5;//from 5 to 9
    }
    default boolean isExpirationPresent() {
        return getExpirationTimeoutInSeconds() != EXPIRE_INSTANTLY;
    }
    default int getExpirationFieldOffset() {
        return getExpirationFieldOffset(isConversationIdPresent(), isExpirationPresent());
    }
    static int getExpirationFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return -1;
        if(conversationIdFieldPresent) return 9;//from 9 to 11
        return 5;//from 5 to 7
    }
    default int getLongPartIndexAndSizeFieldOffset() {
        return getLongPartIndexAndSizeFieldOffset(isConversationIdPresent(), isExpirationPresent(), isLongPart());
    }
    static int getLongPartIndexAndSizeFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean isLongPart) {
        if(!isLongPart) return -1;
        if(conversationIdFieldPresent && expirationFieldPresent) return 11; //5+4+2=11, from 11 to 19
        if(conversationIdFieldPresent) return 9; //from 9 to 17
        if(expirationFieldPresent) return 7; //from 7 to 15
        return 5; //from 5 to 13
    }

    static int readTypeFromHeader(byte[] raw) {
        return BitHelper.getInt32From(raw, HEADER_BYTES_OFFSET_TYPE);
    }
    static int readConversationIdFromHeader(byte[] raw, boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return P2LNode.NO_CONVERSATION_ID;
        return BitHelper.getInt32From(raw, getConversationIdFieldOffset(conversationIdFieldPresent));
    }
    static short readExpirationFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return EXPIRE_INSTANTLY;
        return BitHelper.getInt16From(raw, getExpirationFieldOffset(conversationIdFieldPresent, expirationFieldPresent));
    }
    static int readPartIndexFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean isLongPart) {
        if(!isLongPart) return 0;
        return BitHelper.getInt32From(raw, getLongPartIndexAndSizeFieldOffset(conversationIdFieldPresent, expirationFieldPresent, isLongPart));
    }
    static int readPartSizeFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean isLongPart) {
        if(!isLongPart) return 0;
        return BitHelper.getInt32From(raw, getLongPartIndexAndSizeFieldOffset(conversationIdFieldPresent, expirationFieldPresent, isLongPart)+4);
    }
    
    
    static boolean readConversationIdPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT) == 1;
    }
    static boolean readExpirationPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT) == 1;
    }
    static boolean readIsLongPartFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART) == 1;
    }
    static boolean readIsStreamPartFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART) == 1;
    }
    static boolean readRequestReceiptFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT) == 1;
    }
    static boolean readIsReceiptFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT) == 1;
    }

    int HEADER_OFFSET_FLAG_BYTE = 0;
    int HEADER_BYTES_OFFSET_TYPE = 1;

    int HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT = 0;
    int HEADER_FLAG_BIT_OFFSET_IS_RECEIPT = 1;
    int HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT = 2;
    int HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT = 3;
    int HEADER_FLAG_BIT_OFFSET_IS_LONG_PART = 4;
    int HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART  = 5;
    int HEADER_FLAG_BIT_OFFSET_Y = 6;
    int HEADER_FLAG_BIT_OFFSET_Z = 7;
}
