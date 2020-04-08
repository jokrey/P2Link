package jokrey.utilities.network.link2peer.node.message_headers;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.*;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
     * Type of the message. A shortcut for applications to determine what this message represents without decoding the data field.
     * Also used to wait-for/expect certain types of messages and assigning custom handlers.
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









    default P2LBroadcastMessage generateBroadcastMessage(P2Link source, byte[] payload) {
        return new P2LBroadcastMessage(this, source, null, generateRaw(payload), payload.length);
    }
    default P2LMessage generateMessage(byte[] payload) {
        return new P2LMessage(this, null, generateRaw(payload), payload.length);
    }
    default P2LMessage generateMessage(int payloadLength) {
        return new P2LMessage(this, null, generateRaw(payloadLength), payloadLength);
    }
    default ReceivedP2LMessage generateMockReceived(InetSocketAddress mockSender) {
        return new ReceivedP2LMessage(mockSender, this, null, generateRaw(0), 0);
    }
    default byte[] generateRaw(byte[] payload) {
        byte[] raw = generateRaw(payload.length);
        writeTo(raw, 0);
        System.arraycopy(payload, 0, raw, getSize(), payload.length);
        return raw;
    }
    default byte[] generateRaw(int payloadLength) {
        byte[] raw = new byte[getSize() + payloadLength];
        writeTo(raw, 0);
        return raw;
    }
    default byte[] generateRaw(int payloadLength, int maxRawSize) {
        byte[] raw = new byte[Math.min(maxRawSize, getSize() + payloadLength)];
        writeTo(raw, 0);
        return raw;
    }
    static short toShort(int i) {
        if(i<Short.MIN_VALUE || i>Short.MAX_VALUE) throw new IllegalArgumentException("integer("+i+") was illegally out of short value range");
        return (short) i;
    }
    default int writeTo(byte[] raw, int offset) {
        int highestByteWritten = offset+3; //wrote type
        BitHelper.writeInt16(raw, HEADER_BYTES_OFFSET_TYPE + offset, getType());
        int conversationIdFieldOffset = getConversationIdFieldOffset() + offset;
        int expirationFieldOffset = getExpirationFieldOffset() + offset;
        int stepFieldOffset = getStepFieldOffset() + offset;
        int indexFieldOffset = getPartIndexFieldOffset() + offset;
        int numPartsFieldOffset = getLongNumPartsFieldOffset() + offset;

        byte flagByte = 0;

        if(isConversationIdPresent()) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_CONVERSATION_ID_PRESENT);
            BitHelper.writeInt16(raw, conversationIdFieldOffset, getConversationId());
            highestByteWritten = Math.max(highestByteWritten, conversationIdFieldOffset+2);
        }
        if(isExpirationPresent()) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_EXPIRATION_PRESENT);
            BitHelper.writeInt16(raw, expirationFieldOffset, getExpiresAfter());
            highestByteWritten = Math.max(highestByteWritten, expirationFieldOffset+2);
        }
        if(isStepPresent()) {
            flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STEP_PRESENT);
            BitHelper.writeInt16(raw, stepFieldOffset, getStep());
            highestByteWritten = Math.max(highestByteWritten, stepFieldOffset+2);
        }
        if(isPartIndexFieldPresent()) {
            BitHelper.writeInt32(raw, indexFieldOffset, getPartIndex());
            highestByteWritten = Math.max(highestByteWritten, indexFieldOffset+4);
        }
        if(isLongPart()) {
            BitHelper.writeInt32(raw, numPartsFieldOffset, getNumberOfParts());
            highestByteWritten = Math.max(highestByteWritten, numPartsFieldOffset+4);
        }

        if(isLongPart()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_LONG_PART);
        if(isStreamPart()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_PART);
        if(isStreamEof()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_STREAM_EOF);
        if(isReceipt()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_IS_RECEIPT);
        if(requestReceipt()) flagByte = BitHelper.setBit(flagByte, HEADER_FLAG_BIT_OFFSET_REQUEST_RECEIPT);
        raw[HEADER_OFFSET_FLAG_BYTE + offset] = flagByte;

        return highestByteWritten;
    }
    static P2LMessageHeader from(byte[] raw, int offset) {
        short type = readTypeFromHeader(raw, offset);
        byte flagByte = raw[HEADER_OFFSET_FLAG_BYTE + offset];

        boolean conversationIdFieldPresent = readConversationIdPresentFromHeader(flagByte);
        boolean expirationFieldPresent = readExpirationPresentFromHeader(flagByte);
        boolean stepFieldPresent = readStepPresentFromHeader(flagByte);
        boolean isLongPart = readIsLongPartFromHeader(flagByte);
        boolean requestReceipt = readRequestReceiptFromHeader(flagByte);
        boolean isReceipt = readIsReceiptFromHeader(flagByte);
        boolean isStreamPart = readIsStreamPartFromHeader(flagByte);
        boolean isStreamEof = readIsStreamEofFromHeader(flagByte);

        short conversationId = readConversationIdFromHeader(raw, offset, conversationIdFieldPresent);
        short expiresAfter = readExpirationFromHeader(raw, offset, conversationIdFieldPresent, expirationFieldPresent);
        short step = readStepFromHeader(raw, offset, conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent);
        int partIndex = readPartIndexFromHeader(raw, offset, conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart, isStreamPart, isReceipt);
        int partNumberOfParts = readPartSizeFromHeader(raw, offset, conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart);

        return from(type, conversationId, expiresAfter, step, partIndex, partNumberOfParts, requestReceipt, isReceipt, isLongPart, isStreamPart, isStreamEof);
    }
    static P2LMessageHeader from(short type, short conversationId, short expiresAfter) {
        return from(type, conversationId, expiresAfter, NO_STEP, false);
    }
    static P2LMessageHeader from(short type, short conversationId, short expiresAfter, short step, boolean requestReceipt) {
        boolean conversationIdFieldPresent = conversationId != NO_CONVERSATION_ID;
        boolean expirationFieldPresent = expiresAfter != EXPIRE_INSTANTLY;
        boolean stepFieldPresent = step != NO_STEP;

        if(stepFieldPresent)
            return new ConversationHeader(type, conversationId, step, requestReceipt);
        if(conversationIdFieldPresent && expirationFieldPresent)
            return new FullShortMessageHeader(type, conversationId, expiresAfter, requestReceipt);
        if(conversationIdFieldPresent)
            return new ConversationIdHeader(type, conversationId, requestReceipt);
        if(expirationFieldPresent)
            return new CustomExpirationHeader(type, expiresAfter, requestReceipt);
        return new MinimalHeader(type, requestReceipt);
    }
    static P2LMessageHeader from(short type, short conversationId, short expiresAfter, short step,
                                 int partIndex, int partNumberOfParts,
                                 boolean requestReceipt, boolean isReceipt, boolean isLongPart, boolean isStreamPart, boolean isStreamEof) {
        boolean conversationIdFieldPresent = conversationId != NO_CONVERSATION_ID;
        boolean expirationFieldPresent = expiresAfter != EXPIRE_INSTANTLY;
        boolean stepFieldPresent = step != NO_STEP;

        if(isStreamPart) {
            if(isReceipt)
                return new StreamReceiptHeader(type, conversationId, step, isStreamEof);
            else {
                return new StreamPartHeader(type, conversationId, step, partIndex, requestReceipt, isStreamEof);
            }
        }

        if(isReceipt) {
            if (stepFieldPresent)
                return new ReceiptHeader(type, conversationId, step, requestReceipt);
            else
                return new ReceiptHeader(type, conversationId, NO_STEP, requestReceipt);
        } else if(isLongPart) {
            return new LongMessagePartHeader(type, conversationId, expiresAfter, step, partIndex, partNumberOfParts, requestReceipt);
        } else if(stepFieldPresent) {
            return new ConversationHeader(type, conversationId, step, requestReceipt);
        }

        if(conversationIdFieldPresent && expirationFieldPresent)
            return new FullShortMessageHeader(type, conversationId, expiresAfter, requestReceipt);
        if(conversationIdFieldPresent)
            return new ConversationIdHeader(type, conversationId, requestReceipt);
        if(expirationFieldPresent)
            return new CustomExpirationHeader(type, expiresAfter, requestReceipt);
//        if(!conversationIdFieldPresent && !expirationFieldPresent) //no need, always true
            return new MinimalHeader(type, requestReceipt);

//        return new P2LMessageHeaderFull(type, conversationId, expiresAfter, partIndex, partNumberOfParts, isReceipt, isLongPart, isStreamPart, isStreamEof);
    }
    default P2LMessageHeader toShortMessageHeader() {
        return P2LMessageHeader.from(getType(), getConversationId(), getExpiresAfter(), getStep(), requestReceipt());
    }
    default P2LMessageHeader toMessagePartHeader(int index, int size) {
        return new LongMessagePartHeader(getType(), getConversationId(), getExpiresAfter(), getStep(), index, size, requestReceipt());
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

    default boolean isPartIndexFieldPresent() {
        return isLongPart()||(isStreamPart()&&!isReceipt());
    }
    default int getPartIndexFieldOffset() {
        return getPartIndexFieldOffset(isConversationIdPresent(), isExpirationPresent(), isStepPresent(), isLongPart(), isStreamPart(), isReceipt());
    }
    static int getPartIndexFieldOffset(boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart, boolean isStreamPart, boolean isReceipt) {
        if(!isLongPart&&(!isStreamPart||isReceipt)) return -1;
        if(conversationIdFieldPresent && expirationFieldPresent && stepFieldPresent) return MIN_SIZE+2+2+2;
        if(atLeastTwo(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent)) return MIN_SIZE+2+2;
        if(conversationIdFieldPresent || expirationFieldPresent || stepFieldPresent) return MIN_SIZE+2;
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

    static short readTypeFromHeader(byte[] raw, int offset) {
        return BitHelper.getInt16From(raw, HEADER_BYTES_OFFSET_TYPE + offset);
    }
    static short readConversationIdFromHeader(byte[] raw, int offset, boolean conversationIdFieldPresent) {
        if(!conversationIdFieldPresent) return NO_CONVERSATION_ID;
        return BitHelper.getInt16From(raw, getConversationIdFieldOffset(conversationIdFieldPresent) + offset);
    }
    static short readExpirationFromHeader(byte[] raw, int offset, boolean conversationIdFieldPresent, boolean expirationFieldPresent) {
        if(!expirationFieldPresent) return EXPIRE_INSTANTLY;
        return BitHelper.getInt16From(raw, getExpirationFieldOffset(conversationIdFieldPresent, expirationFieldPresent) + offset);
    }
    static short readStepFromHeader(byte[] raw, int offset, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent) {
        if(!stepFieldPresent) return -1;
        return BitHelper.getInt16From(raw, getStepFieldOffset(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent) + offset);
    }

    static int readPartIndexFromHeader(byte[] raw, int offset, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart, boolean isStreamPart, boolean isReceipt) {
        if(!isLongPart && (!isStreamPart||isReceipt)) return 0;
        return BitHelper.getInt32From(raw, getPartIndexFieldOffset(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart, isStreamPart, isReceipt) + offset);
    }
    static int readPartSizeFromHeader(byte[] raw, int offset, boolean conversationIdFieldPresent, boolean expirationFieldPresent, boolean stepFieldPresent, boolean isLongPart) {
        if(!isLongPart) return 0;
        return BitHelper.getInt32From(raw, getLongNumPartsFieldOffset(conversationIdFieldPresent, expirationFieldPresent, stepFieldPresent, isLongPart) + offset);
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



    /** Encodes the given string as sole content of a newly created message encoder (to be decoded using {@link MessageEncoder#asString()} */
    default MessageEncoder encodeSingle(String payload) {
        return encodeSingle(payload.getBytes(StandardCharsets.UTF_8));
    }
    /** Encodes the given bytes as sole content of a newly created message encoder (to be decoded using {@link MessageEncoder#asBytes()} */
    default MessageEncoder encodeSingle(byte[] bytes) {
        MessageEncoder me = new MessageEncoder(getSize(), getSize() + bytes.length);
        me.setBytes(bytes);
        return me;
    }
    /** Encodes the given payloads into a new message encoder object with the correct offset for it to be directly passed into this conversations methods */
    default MessageEncoder encode(Object... payloads) { return MessageEncoder.encodeAll(getSize(), payloads); }
    /** {@link #encoder(int)}, with initial capacity set to 64 bytes */
    default MessageEncoder encoder() {return encoder( 64);}
    /**
     * Creates a new message encoder with the correct offset for it to be directly passed into this conversations methods with arbitrary encoded data.
     * @param initial_capacity the initial capacity of the byte array backing the encoded data
     */
    default MessageEncoder encoder(int initial_capacity) {return new MessageEncoder(getSize(), initial_capacity);}


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
    class SenderTypeIdentifier extends TypeIdentifier {
        public final InetSocketAddress from;
        public SenderTypeIdentifier(InetSocketAddress from, short messageType) {
            super(messageType);
            this.from = from;
        }
        public SenderTypeIdentifier(ReceivedP2LMessage msg) {
            this(msg.sender, msg.header.getType());
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            SenderTypeIdentifier that = (SenderTypeIdentifier) o;
            return Objects.equals(from, that.from);
        }
        @Override public int hashCode() { return Objects.hash(super.hashCode(), from); }
        @Override public String toString() {
            return "SenderTypeIdentifier{from=" + from + ", messageType=" + super.messageType + '}';
        }
    }
    class BroadcastSourceTypeIdentifier extends TypeIdentifier {
        public final P2Link source;
        public BroadcastSourceTypeIdentifier(P2Link source, short messageType) {
            super(messageType);
            this.source = source;
        }
        public BroadcastSourceTypeIdentifier(P2LBroadcastMessage msg) {
            this(msg.source, msg.header.getType());
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            BroadcastSourceTypeIdentifier that = (BroadcastSourceTypeIdentifier) o;
            return Objects.equals(source, that.source);
        }
        @Override public int hashCode() { return Objects.hash(super.hashCode(), source); }
        @Override public String toString() {
            return "BroadcastSourceTypeIdentifier{source=" + source + ", messageType=" + super.messageType + '}';
        }
    }
    class SenderTypeConversationIdentifier extends SenderTypeIdentifier {
        public final short conversationId;
        public SenderTypeConversationIdentifier(InetSocketAddress from, short messageType, short conversationId) {
            super(from, messageType);
            this.conversationId = conversationId;
        }
        public SenderTypeConversationIdentifier(ReceivedP2LMessage msg) {
            super(msg);
            this.conversationId = msg.header.getConversationId();
        }
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            SenderTypeConversationIdentifier that = (SenderTypeConversationIdentifier) o;
            return conversationId == that.conversationId && Objects.equals(from, that.from);
        }
        @Override public int hashCode() { return Objects.hash(super.hashCode(), conversationId); }
        @Override public String toString() {
            return "SenderTypeConversationIdentifier{from=" + from + ", messageType=" + super.messageType + ", conversationId=" + conversationId + '}';
        }
    }
    class SenderTypeConversationIdStepIdentifier extends SenderTypeConversationIdentifier {
        public final short step;
        public SenderTypeConversationIdStepIdentifier(InetSocketAddress from, short messageType, short conversationId, short step) {
            super(from, messageType, conversationId);
            this.step = step;
        }
        public SenderTypeConversationIdStepIdentifier(ReceivedP2LMessage msg) {
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
        public StepReceiptIdentifier(InetSocketAddress from, short messageType, short conversationId, short step) {
            super(from, messageType, conversationId, step);
        }
        public StepReceiptIdentifier(ReceivedP2LMessage msg) {
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
        public ReceiptIdentifier(InetSocketAddress from, short messageType, short conversationId) {
            super(from, messageType, conversationId);
        }
        public ReceiptIdentifier(ReceivedP2LMessage msg) {
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
    static boolean atLeastTwo(boolean a, boolean b, boolean c) {
        return a && (b || c) || (b && c);
    }
}
