package jokrey.utilities.network.link2peer;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.encoder.as_union.li.LIPosition;
import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.encoder.tag_based.implementation.paired.length_indicator.type.transformer.LITypeToBytesTransformer;
import jokrey.utilities.encoder.type_transformer.bytes.TypeToBytesTransformer;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.util.Hash;

import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.isInternalMessageId;

/**
 * A message always has a sender and data. What actual data is transported is naturally arbitrary.
 * A message also always comes with an type.
 * For non-internal user messages, the type will always be a positive integer - as negative type's are reserved for internal usage (such as ping and broadcast conversations).
 *
 * Message types are used by applications to wait for specific messages.
 *
 * The payload of a message is the actual data transported. It does not include the header(which currently just consists of the message type).
 * This class provides some out of the box methods to decode the payload in a very efficient manor.
 * That allows sending logically disconnected packets of data in a single message.
 *
 *
 *
 * @author jokrey
 */
public class P2LMessage {
    public static int MAX_UDP_PACKET_SIZE = 65507; //65507 is the HARD limitation on windows, the number can be set lower by the app developer to safe memory

    /**
     * Sender of the message
     * for individual messages this will be the peer the message was received from
     * for broadcast messages this will be the peer that originally began distributing the message
     */
    public final String sender;

    public InetSocketAddress senderAsSocketAddress() {
        if(sender == null) return null;
        String[] split = sender.split(":");
        return new InetSocketAddress(split[0], Integer.parseInt(split[1]));
    }

    /**
     * Type of the message. A shortcut for applications to determine what this message represents without decoding the data field.
     * Also used to wait-for/expect certain types of messages and assigning custom handlers.
     */
    public final int type;
    public final int conversationId;
    public final boolean requestReceipt;
    public final boolean isReceipt;

    public static final short INSTANT_TIMEOUT = 0;
    public static final short MAX_TIMEOUT = Short.MAX_VALUE;
    public final short expirationTimeoutInSeconds;
    public boolean isExpired() {
        return receivedAt>0 && (expirationTimeoutInSeconds <= 0 || (System.currentTimeMillis() - receivedAt)/1e3 > expirationTimeoutInSeconds);
    }
    private long receivedAt=-1;
    private void setReceived() {
        if(receivedAt!=-1) throw new IllegalStateException("already marked as received");
        receivedAt = System.currentTimeMillis();
    }

    /**
     * The data field used to communicate arbitrary data between peers.
     */
    private final byte[] raw;

    /** number of bytes in the payload - not neccessarily raw.length-HeaderSize, because raw can sometimes be a larger buffer */
    public final int payloadLength;

    private Hash contentHash;
    /** @return a cached version of the contentHash of this message. The contentHash is 20 bytes long(sha1), usable with contentHash map and includes sender, type and data. */
    public Hash getContentHash() {
        if(contentHash == null)
            contentHash = Hash.contentHashFrom(sender, raw, payloadLength); //no need for thread safety, same value computed in worst case
        return contentHash;
    }

    private byte[] payload;
    /** decodes all payload bytes - the returned array is cached and will be returned when this method is called again - the returned array should NOT BE MUTATED */
    public byte[] asBytes() {
        if(payload == null) //no need for thread safety measures since the same value is calculated...
            payload = Arrays.copyOfRange(raw, HEADER_SIZE, HEADER_SIZE+payloadLength);
        return payload;
    }

    /** Create a new P2LMessage */
    private P2LMessage(String sender,
                       int type, int conversationId, boolean requestReceipt, boolean isReceipt, short expirationTimeoutInSeconds,
                       byte[] raw, int payloadLength, byte[] payload, Hash contentHash) {
        this.sender = sender;
        this.type = type;
        this.conversationId = conversationId;
        this.requestReceipt = requestReceipt;
        this.isReceipt = isReceipt;
        this.expirationTimeoutInSeconds = expirationTimeoutInSeconds;
        this.raw = raw;
        this.payloadLength = payloadLength;
        this.payload = payload;
        this.contentHash = contentHash;

        if(raw.length > MAX_UDP_PACKET_SIZE)
            throw new IllegalArgumentException("total size of a udp packet cannot exceed "+MAX_UDP_PACKET_SIZE+" - size here is: "+raw.length);
    }

    /**
     * Creates an empty message to be send(i.e. sender is not set and will be determined by the node automatically).
     * The resulting message does not have a payload. This is typically useful in messages used for synchronization or receive receipts.
     * @param type message type - used to efficiently distinguish messages, allowing applications to wait for them
     * @return the new message
     */
    public static P2LMessage createSendMessage(int type) {
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, MAX_TIMEOUT, 0);
    }
    public static P2LMessage createSendMessage(int type, int conversationId) {
        return createSendMessageWith(type, conversationId, MAX_TIMEOUT, 0);
    }
    public static P2LMessage createSendMessage(int type, short expirationTimeoutInSeconds) {
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, expirationTimeoutInSeconds, 0);
    }
    public static P2LMessage createSendMessage(int type, int conversationId, short expirationTimeoutInSeconds) {
        return createSendMessageWith(type, conversationId, expirationTimeoutInSeconds, 0);
    }

    /**
     * Creates a message to be send(i.e. sender is not set and will be determined by the node automatically).
     * @param type message type - used to efficiently distinguish messages, allowing applications to wait for them
     * @param payload the payload of the message. The payload can be anything, but it's size cannot not exceed {@link #MAX_UDP_PACKET_SIZE} - {@link #HEADER_SIZE}.
     * @return the new message
     */
    public static P2LMessage createSendMessage(int type, byte[] payload) {
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, MAX_TIMEOUT, payload.length, payload);
    }
    public static P2LMessage createSendMessage(int type, int conversationId, byte[] payload) {
        return createSendMessageWith(type, conversationId, MAX_TIMEOUT, payload.length, payload);
    }
    public static P2LMessage createSendMessage(int type, short expirationTimeoutInSeconds, byte[] payload) {
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, expirationTimeoutInSeconds, payload.length, payload);
    }
    public static P2LMessage createSendMessage(int type, int conversationId, short expirationTimeoutInSeconds, byte[] payload) {
        return createSendMessageWith(type, conversationId, expirationTimeoutInSeconds, payload.length, payload);
    }

    /**
     * Creates a message to be send(i.e. sender is not set and will be determined by the node automatically).
     * @param type message type - used to efficiently distinguish messages, allowing applications to wait for them
     * @param payloads the payloads of the message. The serialized payload is efficiently assembled from the given payload parts.
     * @return the new message
     */
    public static P2LMessage createSendMessageWith(int type, int conversationId, short expirationTimeoutInSeconds, int totalPayloadSize, byte[]... payloads) {
        byte[] raw = new byte[HEADER_SIZE + totalPayloadSize];
        writeHeader(raw, type, conversationId, false, false, expirationTimeoutInSeconds);
        int index = HEADER_SIZE;
        for(byte[] payload : payloads) {
            System.arraycopy(payload, 0, raw, index, payload.length);
            index+=payload.length;
        }

        if(raw.length > 512)
            System.err.println("message greater than 512 bytes - this can be considered inefficient because intermediate low level protocols might break it up - size here is: "+raw.length);

        return new P2LMessage(null, type, conversationId, false, false, expirationTimeoutInSeconds, raw, totalPayloadSize, payloads.length==1?payloads[0]:null, null); //sender does not need to be set on send messages - it is automatically determined by the received from the ip header of the packet
    }


    public static P2LMessage createSendMessage(int type, Object payload) {
        return createSendMessage(type, trans.transform(payload));
    }
    public static P2LMessage createSendMessageFrom(int type, Object... payloads) {
        byte[][] total = new byte[payloads.length][];
        int sizeCounter = 0;
        for(int i=0;i<payloads.length;i++) {
            total[i] = trans.transform(payloads[i]);
            sizeCounter+=total[i].length;
        }
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, MAX_TIMEOUT, sizeCounter, total);
    }
    public static P2LMessage createSendMessageFromVariables(int type, Object... payloads) {
        byte[][] total = new byte[payloads.length*2][];
        int sizeCounter = 0;
        for(int i=0;i<total.length;i+=2) {
            total[i+1] = trans.transform(payloads[i/2]);
            total[i] = makeVariableIndicatorFor(total[i+1].length);
            sizeCounter+=total[i].length + total[i+1].length;
        }
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, MAX_TIMEOUT, sizeCounter, total);
    }
    public static P2LMessage createSendMessageFromVariables(int type, Collection payloads) {
        Iterator payloadsIterator = payloads.iterator();
        byte[][] total = new byte[payloads.size()*2][];
        int sizeCounter = 0;
        for(int i=0;i<total.length;i+=2) {
            total[i + 1] = trans.transform(payloadsIterator.next());
            total[i] = makeVariableIndicatorFor(total[i + 1].length);
            sizeCounter += total[i].length + total[i + 1].length;
        }
        return createSendMessageWith(type, P2LNode.NO_CONVERSATION_ID, MAX_TIMEOUT, sizeCounter, total);
    }

    public static P2LMessage createBroadcast(String sender, int brdMsgType, Object payload) {
        return createBroadcast(sender, brdMsgType, trans.transform(payload));
    }
    public static P2LMessage createBroadcast(String sender, int brdMsgType, byte[] payload) {
        byte[] raw = new byte[HEADER_SIZE + payload.length];
        writeHeader(raw, brdMsgType, 0, false, false, MAX_TIMEOUT);
        System.arraycopy(payload, 0, raw, HEADER_SIZE, payload.length);
        return new P2LMessage(sender, brdMsgType, P2LNode.NO_CONVERSATION_ID,false, false, MAX_TIMEOUT, raw, payload.length, payload, null);
    }

    public static P2LMessage createReceiptFor(P2LMessage message) {
        Hash receiptHash = Hash.contentHashFrom(null, message.raw, message.payloadLength);
        byte[] raw = new byte[HEADER_SIZE + receiptHash.length()];
        writeHeader(raw, message.type, message.conversationId, false, true, INSTANT_TIMEOUT); //receipts instantly time out - because they are always automatically waited on by the system..
        System.arraycopy(receiptHash.raw(), 0, raw, HEADER_SIZE, receiptHash.length());
        return new P2LMessage(null, message.type, message.conversationId, false,true, INSTANT_TIMEOUT, raw, receiptHash.length(), receiptHash.raw(), null);
    }
    public boolean validateIsReceiptFor(P2LMessage message) {
        if(!isReceipt) throw new IllegalStateException("cannot validate receipt, since this is not a receipt");
        Hash receiptHash = Hash.contentHashFrom(null, message.raw, message.payloadLength);
        return payloadEquals(receiptHash.raw());
    }

    /** @return a udp datagram packet from the internal data - it can be decoded on the receiver side using {@link #fromPacket(DatagramPacket)}
     * @param to*/
    public DatagramPacket getPacket(SocketAddress to) {
        return new DatagramPacket(raw, raw.length, to);
    }
    /** @return Decodes a udp datagram packet into a p2l message */
    public static P2LMessage fromPacket(DatagramPacket packet) {
        byte[] raw = packet.getData();
        int type = readTypeFromHeader(raw);
        int conversationId = readConversationIdFromHeader(raw);
        short expirationTimeoutInSeconds = readExpirationTimeoutInSeconds(raw);
        boolean requestReceipt = readRequestReceiptFromHeader(raw);
        boolean isReceipt = readIsReceiptFromHeader(raw);

        if(raw.length > packet.getLength()*2 && raw.length > 8192)
            raw =  Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
        P2LMessage msg = new P2LMessage(WhoAmIProtocol.toString(packet.getSocketAddress()), type, conversationId, requestReceipt, isReceipt, expirationTimeoutInSeconds, raw, packet.getLength()-HEADER_SIZE, null, null); //contentHash and payload are only calculated if required... Preferably the 'as' methods should be used to extract data.
        msg.setReceived();
        return msg;
    }


    public boolean payloadEquals(byte[] o) {
        if(payloadLength != o.length) return false;
        for(int i=0;i<o.length;i++)
            if(raw[HEADER_SIZE+i] != o[i])
                return false;
        return true;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LMessage that = (P2LMessage) o;
        return Objects.equals(sender, that.sender) && Arrays.equals(raw, that.raw) && type == that.type && isReceipt == that.isReceipt;
    }
    @Override public int hashCode() {
        return Arrays.hashCode(raw);
    }
    @Override public String toString() {
        return "P2LMessage{sender=" + sender + ", type=" + type + ", requestReceipt=" + requestReceipt + ", isReceipt=" + isReceipt + ", expirationTimeoutInSeconds=" + expirationTimeoutInSeconds + ", raw=" + Arrays.toString(raw) + ", contentHash=" + contentHash + '}';
    }

    //HELPER
    private int pointer=HEADER_SIZE;
    /** resets the internal iterating pointer */
    public void resetReader() {pointer = HEADER_SIZE;}

    /** decodes the next payload byte as a boolean (more context efficient {@link LITypeToBytesTransformer#detransform_boolean(byte[])}) */
    public boolean nextBool() {
        return raw[pointer++] == 1;
    }
    /** decodes the next payload byte as a byte (more context efficient {@link LITypeToBytesTransformer#detransform_byte(byte[])}) */
    public byte nextByte() {
        return raw[pointer++];
    }
    /** decodes the next 4 payload bytes as an integer(32bit) (more context efficient {@link LITypeToBytesTransformer#detransform_int(byte[])}) */
    public int nextInt() {
        int before = pointer;
        pointer+=4; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getInt32From(raw, before);
    }
    /** decodes the next 8 payload bytes as an integer(64bit) (more context efficient {@link LITypeToBytesTransformer#detransform_long(byte[])}) */
    public long nextLong() {
        int before = pointer;
        pointer+=8; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getIntFromNBytes(raw, before, 8);
    }
    /** decodes the next 4 payload bytes as a floating point number(32 bit) (more context efficient {@link LITypeToBytesTransformer#detransform_float(byte[])} (byte[])}) */
    public float nextFloat() {
        int before = pointer;
        pointer+=4; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getFloat32From(raw, before);
    }
    /** decodes the next 8 payload bytes as a floating point number(64 bit) (more context efficient {@link LITypeToBytesTransformer#detransform_double(byte[])} (byte[])}) */
    public double nextDouble() {
        int before = pointer;
        pointer+=8; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getFloat64From(raw, before);
    }
    /** decodes the next n payload bytes as bytes - uses length indicator functionality to determine n (more context efficient {@link LIbae#decode(LIPosition)}) */
    public byte[] nextVariable() {
        long[] li_bounds = LIbae.get_next_li_bounds(raw, pointer, pointer, HEADER_SIZE + payloadLength - 1);
        if(li_bounds == null) return null;
        pointer = (int) li_bounds[1];
        return Arrays.copyOfRange(raw, (int) li_bounds[0], (int) li_bounds[1]);
    }
    /** decodes the next n payload bytes as a utf8 string - uses length indicator functionality to determine n (more context efficient {@link LIbae#decode(LIPosition)}) */
    public String nextVariableString() {
        long[] li_bounds = LIbae.get_next_li_bounds(raw, pointer, pointer, HEADER_SIZE + payloadLength - 1);
        if(li_bounds == null) return null;
        pointer = (int) li_bounds[1];
        return new String(raw, (int) li_bounds[0], (int) (li_bounds[1]-li_bounds[0]), StandardCharsets.UTF_8);
    }
    /**
     * Used to generate a length indicator for a variable payload part. Has to be added to the payload as a payload part before the variable payload part.
     * @param payloadPartLength length of the variable payload part
     * @return variable payload part prefix
     */
    public static byte[] makeVariableIndicatorFor(int payloadPartLength) {
        return LIbae.generateLI(payloadPartLength);
    }

    /** decodes all payload bytes as a utf8 string */
    public String asString() {
        return new String(raw, HEADER_SIZE, payloadLength, StandardCharsets.UTF_8);
    }

    /** Type transformer used internally for type transformations - when encoding standard types into payload parts with this transformer, the message can be efficiently decoded */
    public static final TypeToBytesTransformer trans = new LITypeToBytesTransformer();


    /**
     * @return whether this message is internal or used by the application running on the p2l network
     */
    public boolean isInternalMessage() {
        return isInternalMessageId(type);
    }
    /**
     * Copy the message, but adds a sender. The originally message is not mutated.
     * @param newSender the sender
     * @return the new message
     * @throws IllegalArgumentException if the sender of the message is not null, i.e. already set
     */
    public P2LMessage attachSender(String newSender) {
        if(sender != null)
            throw new IllegalArgumentException("sender already known");
        return new P2LMessage(newSender, type, conversationId, requestReceipt, isReceipt, expirationTimeoutInSeconds, raw, payloadLength, payload, contentHash);
    }
    public P2LMessage mutateToRequestReceipt() {
        writeHeader(raw, type, conversationId, true, isReceipt, expirationTimeoutInSeconds);//todo - MUTATES RAW!! (does not mutate contentHash though)
        return new P2LMessage(sender, type, conversationId, true, isReceipt, expirationTimeoutInSeconds, raw, payloadLength, payload, contentHash);
    }





    public static final int HEADER_SIZE = 11;
    public static final int HEADER_FLAG_BYTE_OFFSET_INDEX = 0;
    public static final int HEADER_EXPIRATION_TIME_BYTES_OFFSET_INDEX = 1;
    public static final int HEADER_TYPE_BYTES_OFFSET_INDEX = 3;
    public static final int HEADER_CONVERSATION_ID_BYTES_OFFSET_INDEX = 7;
    private static void writeHeader(byte[] raw, int type, int conversationId, boolean requestReceipt, boolean isReceipt, short expirationTimeoutInSeconds) {
        BitHelper.writeInt32(raw, HEADER_TYPE_BYTES_OFFSET_INDEX, type);
        BitHelper.writeInt32(raw, HEADER_CONVERSATION_ID_BYTES_OFFSET_INDEX, conversationId);
        BitHelper.writeInt16(raw, HEADER_EXPIRATION_TIME_BYTES_OFFSET_INDEX, expirationTimeoutInSeconds);
        byte flagByte = 0;
        if(requestReceipt) flagByte = BitHelper.setBit(flagByte, 0);
        if(isReceipt) flagByte = BitHelper.setBit(flagByte, 1);
        raw[HEADER_FLAG_BYTE_OFFSET_INDEX] = flagByte;
    }
    private static int readTypeFromHeader(byte[] raw) {
        return BitHelper.getInt32From(raw, HEADER_TYPE_BYTES_OFFSET_INDEX);
    }
    private static int readConversationIdFromHeader(byte[] raw) {
        return BitHelper.getInt32From(raw, HEADER_CONVERSATION_ID_BYTES_OFFSET_INDEX);
    }
    private static short readExpirationTimeoutInSeconds(byte[] raw) {
        return BitHelper.getInt16From(raw, HEADER_EXPIRATION_TIME_BYTES_OFFSET_INDEX);
    }
    private static boolean readRequestReceiptFromHeader(byte[] raw) {
        return BitHelper.getBit(raw[HEADER_FLAG_BYTE_OFFSET_INDEX], 0) == 1;
    }
    private static boolean readIsReceiptFromHeader(byte[] raw) {
        return BitHelper.getBit(raw[HEADER_FLAG_BYTE_OFFSET_INDEX], 1) == 1;
    }
}
