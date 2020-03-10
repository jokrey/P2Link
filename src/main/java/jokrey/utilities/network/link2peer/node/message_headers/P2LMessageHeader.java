package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.Hash;

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
 *    1(flags) + 4(type) + 4(conversationId, optional) + 2(expiration, optional) + 8(long msg, index+size) + 8(udp) = 19 + 8 = 27 bytes (on top of ip)
 * Header size for stream messages(tcp / udt comparable):
 *    1(flags) + 4(type) + (conversationId, optional) + 4(index) + 8(udp) = 9/13 + 8 = 17/21 bytes (on top of ip)
 *    compared to ip that is pretty good, additionally this protocol allows 2^31-1(*2^31-1) simultaneous streaming connections with the same peer
 *    (on the other hand it expects less package loss and is expected to behave less well with congestion)
 *
 *
 * @author jokrey
 */
public interface P2LMessageHeader {
    short NO_CONVERSATION_ID = 0;
    short NO_STEP = -1;

    /**
     * Sender of the message
     * for individual messages this will be the peer the message was received from (automatically determined from the udp package sender address)
     * for broadcast messages this will be the peer that originally began distributing the message
     */
    P2Link getSender();

    /**
     * Type of the message. A shortcut for applications to determine what this message represents without decoding the data field.
     * Also used to wait-for/expect certain types of messages and assigning custom handlers.
     *
     * TODO - reduce type range to 8 bit - otherwise the type might be misused (and typical application do not require many different types of messages)
     */
    short getType();
    /**
     * Conversation id
     * Unique id that should be created using {@link P2LNode#createUniqueConversationId()}, when a conversation is established.
     * All subsequent messages in the conversation should use the id.
     * When retrying a conversation after package loss or canceling a conversation, this id can be used to distinguish messages in the queues.
     */
    short getConversationId();

    /**
     * Step
     * Unique id that should be created using {@link P2LNode#createUniqueConversationId()}, when a conversation is established.
     * All subsequent messages in the conversation should use the id.
     * When retrying a conversation after package loss or canceling a conversation, this id can be used to distinguish messages in the queues.
     */
    short getStep();

    /**
     * The time in seconds until this message is removed from the message queues
     * For value <= 0 the message will never be added to the message queue, it is only considered if a consumer is waiting when it arrives.
     */
    short getExpiresAfter();
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
    boolean isStreamEof();

    //cannot be merged, index 0 is not necessarily the first packet received
    /** The part index (either stream part or long part). Internal use only. */
    int getPartIndex();
    /** The total number of parts in a split long message. Internal use only. */
    int getNumberOfParts();


    default boolean isConversationPart() {
        return getStep() != NO_STEP;
    }


    /** Internal use only. */
    void mutateToRequestReceipt(byte[] raw);

    default boolean equalsIgnoreVolatile(P2LMessageHeader that) {
        return  getType() == that.getType() && getConversationId() == that.getConversationId() &&
                getPartIndex() == that.getPartIndex() && getNumberOfParts() == that.getNumberOfParts() &&
                requestReceipt() == that.requestReceipt() && isReceipt() == that.isReceipt() && isLongPart() == that.isLongPart() &&
                getExpiresAfter() == that.getExpiresAfter();
    }
    default Hash contentHashFrom(byte[] raw, int payloadLength) {
        return contentHashFrom(getSender().getStringRepresentation(), raw, payloadLength);
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
            if(getConversationIdFieldOffset() != -1)
                hashFunction.update(raw, getConversationIdFieldOffset(), 4); //conversation id
            if(payloadLength > 0)
                hashFunction.update(raw, getSize(), payloadLength); //only payload
            return new Hash(hashFunction.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new Error("missing critical algorithm");
        }
    }










    default P2LMessage generateMessage(byte[] payload) {
        return new P2LMessage(this, null, generateRaw(payload), payload.length);
    }
    default byte[] generateRaw(byte[] payload) {
        byte[] raw = generateRaw(payload.length);
        writeTo(raw);
        System.arraycopy(payload, 0, raw, getSize(), payload.length);
        return raw;
    }
    default byte[] generateRaw(int payloadLength) {
        byte[] raw = new byte[getSize() + payloadLength];
        writeTo(raw);
        return raw;
    }
    default byte[] generateRaw(int payloadLength, int maxRawSize) {
        byte[] raw = new byte[Math.min(maxRawSize, getSize() + payloadLength)];
        writeTo(raw);
        return raw;
    }
    static short toShort(int i) {
        if(i<Short.MIN_VALUE || i>Short.MAX_VALUE) throw new IllegalArgumentException("integer("+i+") was illegally out of short value range");
        return (short) i;
    }
    default void writeTo(byte[] raw) {
        BitHelper.writeInt16(raw, HEADER_BYTES_OFFSET_TYPE, getType());
        int conversationIdFieldOffset = getConversationIdFieldOffset();
        int expirationFieldOffset = getExpirationFieldOffset();
        int stepFieldOffset = getStepFieldOffset();
        int indexFieldOffset = getPartIndexFieldOffset();
        int numPartsFieldOffset = getLongNumPartsFieldOffset();

        byte flagByte = 0;

        if(conversationIdFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT);
            BitHelper.writeInt16(raw, conversationIdFieldOffset, getConversationId());
        }
        if(expirationFieldOffset != -1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT);
            BitHelper.writeInt16(raw, expirationFieldOffset, getExpiresAfter());
        }
        if(stepFieldOffset!=-1) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STEP_PRESENT);
            BitHelper.writeInt16(raw, stepFieldOffset, getStep());
        }
        if(indexFieldOffset != -1) {
            BitHelper.writeInt32(raw, indexFieldOffset, getPartIndex()); //todo do not encode if is receipt, stream receipt does not need to have an index field
//            System.out.println("indexFieldOffset = " + indexFieldOffset);
//            System.out.println("getPartIndex() = " + getPartIndex());
        }
        if(numPartsFieldOffset != -1) {
            BitHelper.writeInt32(raw, numPartsFieldOffset, getNumberOfParts());
//            System.out.println("numPartsFieldOffset = " + numPartsFieldOffset);
//            System.out.println("getNumberOfParts() = " + getNumberOfParts());
        }

        if(isLongPart()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART);
        if(isStreamPart()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART);
        if(isStreamEof()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_EOF);
        if(isReceipt()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT);
        if(requestReceipt()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        raw[HEADER_OFFSET_FLAG_BYTE] = flagByte;
    }
    static P2LMessageHeader from(byte[] raw, P2Link from) {
        short type = readTypeFromHeader(raw);
        byte flagByte = raw[HEADER_OFFSET_FLAG_BYTE];

        boolean conversationIdFieldPresent = readConversationIdPresentFromHeader(flagByte);
        boolean expirationFieldPresent = readExpirationPresentFromHeader(flagByte);
        boolean stepFieldPresent = readStepPresentFromHeader(flagByte);
        boolean isLongPart = readIsLongPartFromHeader(flagByte);
        boolean requestReceipt = readRequestReceiptFromHeader(flagByte);
        boolean isReceipt = readIsReceiptFromHeader(flagByte);
        boolean isStreamPart = readIsStreamPartFromHeader(flagByte);
        boolean isStreamEof = readIsStreamEofFromHeader(flagByte);

        short conversationId = readConversationIdFromHeader(raw, conversationIdFieldPresent);
        short expiresAfter = readExpirationFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent);
        short step = readStepFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent);
        int partIndex = readPartIndexFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart, isStreamPart, isReceipt);
        int partNumberOfParts = readPartSizeFromHeader(raw, conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart);

//        if(isLongPart) {
//            System.out.println("partIndex = " + partIndex);
//            System.out.println("partNumberOfParts = " + partNumberOfParts);
//        }
        return from(from, type, conversationId, expiresAfter, step, partIndex, partNumberOfParts, requestReceipt, isReceipt, isLongPart, isStreamPart, isStreamEof);
    }
    static P2LMessageHeader from(P2Link sender,
                                 short type, short conversationId, short expiresAfter) {
        return from(sender, type, conversationId, expiresAfter, NO_STEP, false);
    }
    static P2LMessageHeader from(P2Link sender,
                                 short type, short conversationId, short expiresAfter, short step, boolean requestReceipt) {
        boolean conversationIdFieldPresent = conversationId != NO_CONVERSATION_ID;
        boolean expirationFieldPresent = expiresAfter != EXPIRE_INSTANTLY;
        boolean stepFieldPresent = step != NO_STEP;

        if(stepFieldPresent)
            return new ConversationHeader(sender, type, conversationId, step, requestReceipt);
        if(conversationIdFieldPresent && expirationFieldPresent)
            return new FullShortMessageHeader(sender, type, conversationId, expiresAfter, requestReceipt);
        if(conversationIdFieldPresent)
            return new ConversationIdHeader(sender, type, conversationId, requestReceipt);
        if(expirationFieldPresent)
            return new CustomExpirationHeader(sender, type, expiresAfter, requestReceipt);
        return new MinimalHeader(sender, type, requestReceipt);
    }
    static P2LMessageHeader from(P2Link sender,
                                 short type, short conversationId, short expiresAfter, short step,
                                 int partIndex, int partNumberOfParts,
                                 boolean requestReceipt, boolean isReceipt, boolean isLongPart, boolean isStreamPart, boolean isStreamEof) {
        boolean conversationIdFieldPresent = conversationId != NO_CONVERSATION_ID;
        boolean expirationFieldPresent = expiresAfter != EXPIRE_INSTANTLY;
        boolean stepFieldPresent = step != NO_STEP;

        if(isStreamPart) {
            if(isReceipt)
                return new StreamReceiptHeader(sender, type, conversationId, isStreamEof);
            else
                return new StreamPartHeader(sender, type, conversationId, partIndex, requestReceipt, isStreamEof);
        }

        if(isReceipt) {
            if (stepFieldPresent)
                return new ReceiptHeader(sender, type, conversationId, step, requestReceipt);
            else
                return new ReceiptHeader(sender, type, conversationId, NO_STEP, requestReceipt);
        } else if(isLongPart) {
            return new LongMessagePartHeader(sender, type, conversationId, expiresAfter, step, partIndex, partNumberOfParts, requestReceipt);
        } else if(stepFieldPresent) {
            return new ConversationHeader(sender, type, conversationId, step, requestReceipt);
        }

        if(conversationIdFieldPresent && expirationFieldPresent)
            return new FullShortMessageHeader(sender, type, conversationId, expiresAfter, requestReceipt);
        if(conversationIdFieldPresent)
            return new ConversationIdHeader(sender, type, conversationId, requestReceipt);
        if(expirationFieldPresent)
            return new CustomExpirationHeader(sender, type, expiresAfter, requestReceipt);
//        if(!conversationIdFieldPresent && !expirationFieldPresent) //no need, always true
            return new MinimalHeader(sender, type, requestReceipt);
        
//        return new P2LMessageHeaderFull(sender, type, conversationId, expiresAfter, partIndex, partNumberOfParts, isReceipt, isLongPart, isStreamPart, isStreamEof);
    }
    default P2LMessageHeader toShortMessageHeader() {
        return P2LMessageHeader.from(getSender(), getType(), getConversationId(), getExpiresAfter(), getStep(), requestReceipt());
    }
    default P2LMessageHeader toMessagePartHeader(int index, int size) {
        return new LongMessagePartHeader(getSender(), getType(), getConversationId(), getExpiresAfter(), getStep(), index, size, requestReceipt());
    }



    int MIN_SIZE = 3;
    default int getSize() {
        return getSize(isConversationIdPresent(), isExpirationPresent(), isStepPresent(), isLongPart(), isStreamPart(), isReceipt());
    }
    static int getSize(boolean isConversationIdPresent, boolean isExpirationPresent, boolean isStepPresent, boolean isLongPart, boolean isStreamPart, boolean isReceipt) {
        int size = MIN_SIZE;//flag byte + type bytes
        if(isConversationIdPresent) size += 2;
        if(isExpirationPresent) size += 2;
        if(isStepPresent) size += 2;
        //the following 2 are mutually exclusive
        if(isStreamPart && !isReceipt) size += 4; //for index data
        else if(isLongPart) size += 8;
        return size;
    }
    default boolean isConversationIdPresent() {
        return getConversationId() != NO_CONVERSATION_ID;
    }
    default int getConversationIdFieldOffset() { return getConversationIdFieldOffset(isConversationIdPresent()); }
    static int getConversationIdFieldOffset(boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return -1;
        return MIN_SIZE;
    }
    default boolean isExpirationPresent() {
        return getExpiresAfter() != EXPIRE_INSTANTLY;
    }
    default int getExpirationFieldOffset() { return getExpirationFieldOffset(isConversationIdPresent(), isExpirationPresent()); }
    static int getExpirationFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return -1;
        if(conversationIdFieldPresent) return MIN_SIZE+2;
        return MIN_SIZE;
    }
    default boolean isStepPresent() {
        return getStep() != NO_STEP;
    }
    default int getStepFieldOffset() {
        return getStepFieldOffset(isConversationIdPresent(), isExpirationPresent(), isStepPresent());
    }
    static int getStepFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent) {
        if(!stepFieldPresent) return -1;
        if(conversationIdFieldPresent && expirationFieldPresent) return MIN_SIZE+2+2;
        if(conversationIdFieldPresent) return MIN_SIZE+2;
        return MIN_SIZE;
    }

    default int getPartIndexFieldOffset() {
        return getPartIndexFieldOffset(isConversationIdPresent(), isExpirationPresent(), isStepPresent(), isLongPart(), isStreamPart(), isReceipt());
    }
    static int getPartIndexFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart, boolean isStreamPart, boolean isReceipt) {
        if(!isLongPart&&(!isStreamPart||isReceipt)) return -1;
        if(conversationIdFieldPresent && expirationFieldPresent && stepFieldPresent) return MIN_SIZE+2+2+2;
        if(conversationIdFieldPresent && (expirationFieldPresent || stepFieldPresent)) return MIN_SIZE+2+2;
        if(expirationFieldPresent) return MIN_SIZE+2;
        return MIN_SIZE;
    }
    default int getLongNumPartsFieldOffset() {
        return getLongNumPartsFieldOffset(isConversationIdPresent(), isExpirationPresent(), isStepPresent(), isLongPart());
    }
    static int getLongNumPartsFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart) {
        if(!isLongPart) return -1;
        if(conversationIdFieldPresent && expirationFieldPresent && stepFieldPresent) return MIN_SIZE+2+2+2+4;
        if(conversationIdFieldPresent && (expirationFieldPresent || stepFieldPresent)) return MIN_SIZE+2+2+4;
        if(expirationFieldPresent) return MIN_SIZE+2+4;
        return MIN_SIZE+4;
    }

    static short readTypeFromHeader(byte[] raw) {
        return BitHelper.getInt16From(raw, HEADER_BYTES_OFFSET_TYPE);
    }
    static short readConversationIdFromHeader(byte[] raw, boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return NO_CONVERSATION_ID;
        return BitHelper.getInt16From(raw, getConversationIdFieldOffset(conversationIdFieldPresent));
    }
    static short readExpirationFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return EXPIRE_INSTANTLY;
        return BitHelper.getInt16From(raw, getExpirationFieldOffset(conversationIdFieldPresent, expirationFieldPresent));
    }
    static short readStepFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent) {
        if(!stepFieldPresent) return -1;
        return BitHelper.getInt16From(raw, getStepFieldOffset(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent));
    }

    static int readPartIndexFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart, boolean isStreamPart, boolean isReceipt) {
        if(!isLongPart && (!isStreamPart||isReceipt)) return 0;
        return BitHelper.getInt32From(raw, getPartIndexFieldOffset(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart, isStreamPart, isReceipt));
    }
    static int readPartSizeFromHeader(byte[] raw, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart) {
        if(!isLongPart) return 0;
        return BitHelper.getInt32From(raw, getLongNumPartsFieldOffset(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart));
    }
    
    
    static boolean readConversationIdPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT) == 1;
    }
    static boolean readExpirationPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT) == 1;
    }
    static boolean readStepPresentFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STEP_PRESENT) == 1;
    }
    static boolean readIsLongPartFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART) == 1;
    }
    static boolean readIsStreamPartFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART) == 1;
    }
    static boolean readIsStreamEofFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_EOF) == 1;
    }
    static boolean readRequestReceiptFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT) == 1;
    }
    static boolean readIsReceiptFromHeader(byte flagByte) {
        return BitHelper.getBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT) == 1;
    }

    int HEADER_OFFSET_FLAG_BYTE = 0;
    int HEADER_BYTES_OFFSET_TYPE = 1;

    int HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT = 0; //can only be set if stream part is not set,
                                                    // in case of flag bit shortage, could disallow a receipt requesting a receipt - meaning request receipt could not be set if is receipt is set
    int HEADER_FLAG_BIT_OFFSET_IS_RECEIPT = 1; //can only be set if long part and stream part are both NOT set
    int HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT = 2;
    int HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT = 3; // cannot be set if stream part is set
    int HEADER_FLAG_BIT_OFFSET_IS_LONG_PART = 4; // cannot be set if stream part is set
    int HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART  = 5; // cannot be set if long part is set
    int HEADER_FLAG_BIT_OFFSET_IS_STREAM_EOF = 6; //can only be set if streamPart is set (could be merged with isReceipt or request receipt)
    int HEADER_FLAG_BIT_OFFSET_IS_STEP_PRESENT = 7;


    abstract class HeaderIdentifier {
        public abstract boolean equals(Object o);
        public abstract int hashCode();
    }
    class TypeIdentifier extends HeaderIdentifier {
        public final short messageType;
        public TypeIdentifier(short messageType) {
            this.messageType = messageType;
        }
        public TypeIdentifier(P2LMessage msg) {
            this(msg.header.getType());
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypeIdentifier that = (TypeIdentifier) o;
            return messageType == that.messageType;
        }
        @Override public int hashCode() { return Objects.hash(messageType); }
        @Override public String toString() {
            return "TypeIdentifier{messageType=" + messageType + '}';
        }
    }
    class SenderTypeConversationIdentifier extends TypeIdentifier {
        public final SocketAddress from;
        public final short conversationId;
        public SenderTypeConversationIdentifier(SocketAddress from, short messageType, short conversationId) {
            super(messageType);
            this.from = from;
            this.conversationId = conversationId;
        }
        public SenderTypeConversationIdentifier(P2LMessage msg) {
            this(msg.header.getSender().getSocketAddress(), msg.header.getType(), msg.header.getConversationId());
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            SenderTypeConversationIdentifier that = (SenderTypeConversationIdentifier) o;
            return conversationId == that.conversationId && Objects.equals(from, that.from);
        }
        @Override public int hashCode() { return Objects.hash(super.hashCode(), from, conversationId); }
        @Override public String toString() {
            return "SenderTypeConversationIdentifier{from=" + from + ", messageType=" + super.messageType + ", conversationId=" + conversationId + '}';
        }
    }
    class SenderTypeConversationIdStepIdentifier extends SenderTypeConversationIdentifier {
        public final short step;
        public SenderTypeConversationIdStepIdentifier(SocketAddress from, short messageType, short conversationId, short step) {
            super(from, messageType, conversationId);
            this.step = step;
        }
        public SenderTypeConversationIdStepIdentifier(P2LMessage msg) {
            super(msg);
            this.step = msg.header.getStep();
        }
        @Override public boolean equals(Object o) {
            return o instanceof SenderTypeConversationIdStepIdentifier && super.equals(o) && ((SenderTypeConversationIdStepIdentifier)o).step == step;
        }
        @Override public int hashCode() {
            return super.hashCode()*13 + Short.hashCode(step);
        }
        @Override public String toString() {
            return "SenderTypeConversationIdStepIdentifier{from=" + super.from + ", messageType=" + super.messageType + ", conversationId=" + super.conversationId + ", step=" + step + '}';
        }
    }
    class StepReceiptIdentifier extends SenderTypeConversationIdStepIdentifier {
        public StepReceiptIdentifier(SocketAddress from, short messageType, short conversationId, short step) {
            super(from, messageType, conversationId, step);
        }
        public StepReceiptIdentifier(P2LMessage msg) {
            super(msg);
        }
        @Override public boolean equals(Object o) {
            return o instanceof StepReceiptIdentifier && super.equals(o);
        }
        @Override public int hashCode() {
            return super.hashCode()*13;
        }
        @Override public String toString() {
            return "ReceiptIdentifier{from=" + super.from + ", messageType=" + super.messageType + ", conversationId=" + super.conversationId + ", step=" + step + '}';
        }
    }
    class ReceiptIdentifier extends SenderTypeConversationIdentifier {
        public ReceiptIdentifier(SocketAddress from, short messageType, short conversationId) {
            super(from, messageType, conversationId);
        }
        public ReceiptIdentifier(P2LMessage msg) {
            super(msg);
        }
        @Override public boolean equals(Object o) {
            return o instanceof ReceiptIdentifier && super.equals(o);
        }
        @Override public int hashCode() {
            return super.hashCode()*13;
        }
        @Override public String toString() {
            return "ReceiptIdentifier{from=" + super.from + ", messageType=" + super.messageType + ", conversationId=" + super.conversationId + '}';
        }
    }
}
