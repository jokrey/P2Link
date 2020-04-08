package jokrey.utilities.network.link2peer;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.node.message_headers.MinimalHeader;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.node.message_headers.ReceiptHeader;
import jokrey.utilities.network.link2peer.util.Hash;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_NAT_HOLE_PACKET;
import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.isInternalMessageId;
import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.*;

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
public class P2LMessage extends MessageEncoder {
    /**
     * Hard limit for the udp package size.
     * 65507 is the HARD limitation on windows, the number can be set lower by the app developer to safe memory using {@link #CUSTOM_RAW_SIZE_LIMIT}
     */
    public static final int MAX_UDP_PACKET_SIZE = 65507;
    /**
     * This can be set by the user, should larger packages be required.
     * Should be the same between all peers, as the receiver uses it to create the internal buffer and will cut up messages longer than this limit.
     * The limit is should if possible be lower than the
     */
    public static int CUSTOM_RAW_SIZE_LIMIT = 8192; //BELOW IMPEDES STREAMS

    /**
     * Constant for an instant message expiration.
     * An example for message that instantly expire are receipts.
     * All messages that are directly waited upon(for example in a conversation), should instantly timeout.
     */
    public static final short EXPIRE_INSTANTLY = 0;
    /**
     * Node max timeout for messages - also used for incoming messages, so no message will ever remain longer in the message queues - even if the sender expects that.
     * default is at 180 seconds = 3 minutes
     */
    public static short MAX_EXPIRATION_TIMEOUT = 180;
    
    public final P2LMessageHeader header;

    private Hash contentHash;

    /** @return a cached version of the contentHash of this message. The contentHash is 20 bytes long(sha1), usable with contentHash map and includes sender, type and data. */
    public Hash getContentHash() {
        if(contentHash == null) { //no need for thread safety, same value computed twice in worst case
            try {
                MessageDigest hashFunction = MessageDigest.getInstance("SHA-1");
                //TYPE AND PAYLOAD ARE THE CONTENT - EXPIRATION, CONVERSATION ID, STEP, RECEIPT ETC ARE NOT PART OF THE CONTENT
                hashFunction.update(content, HEADER_BYTES_OFFSET_TYPE, 2); //type
                if(getPayloadLength() > 0)
                    hashFunction.update(content, header.getSize(), getPayloadLength()); //only payload
                return new Hash(hashFunction.digest());
            } catch (NoSuchAlgorithmException e) {
                throw new Error("missing critical algorithm");
            }
        }
        return contentHash;
    }

    /** Create a new P2LMessage */
    public P2LMessage(P2LMessageHeader header, Hash contentHash, byte[] raw, int payloadLength) {
        super(true, raw, header.getSize(), header.getSize() + payloadLength);
        this.header = header;
        this.contentHash = contentHash;
    }

    /** @return a udp datagram packet from the internal data - it can be decoded on the receiver side using {@link #fromPacket(InetSocketAddress, DatagramPacket)}
     * @param to receiver of the created datagram packet*/
    public DatagramPacket toPacket(InetSocketAddress to) {
        int actualLength = requiredRawSize();

        if (actualLength > CUSTOM_RAW_SIZE_LIMIT) throw new IllegalArgumentException("total size of raw cannot exceed " + CUSTOM_RAW_SIZE_LIMIT + "(user set limit) - size here is: " + actualLength + " - max payload size is: " + (CUSTOM_RAW_SIZE_LIMIT - header.getSize()));
        if (actualLength > MAX_UDP_PACKET_SIZE) throw new IllegalArgumentException("total size of a udp packet cannot exceed " + MAX_UDP_PACKET_SIZE + " - size here is: " + actualLength);
        return new DatagramPacket(content, actualLength, to);
    }

    private static int getMaxPacketSize() {
        return Math.min(CUSTOM_RAW_SIZE_LIMIT, MAX_UDP_PACKET_SIZE);
    }

    /** @return Decodes a udp datagram packet into a p2l message */
    public static ReceivedP2LMessage fromPacket(InetSocketAddress sender, DatagramPacket packet) {
        byte[] raw = packet.getData();

        P2LMessageHeader header = P2LMessageHeader.from(raw, 0);

        if(raw.length > packet.getLength()*2 && raw.length > 4096) //fixme heuristic
            raw =  Arrays.copyOfRange(packet.getData(), 0, packet.getLength()); //if message is very long copy the byte array to a smaller byte array to save memory

        //contentHash and payload are only calculated if required... Preferably the 'as' methods should be used to extract data.
        return new ReceivedP2LMessage(sender, header, null, raw, packet.getLength()-header.getSize());
    }

    /** @return whether a single packet will suffice to send the data this message contains */
    public boolean canBeSentInSinglePacket() {
        return requiredRawSize() <= getMaxPacketSize();
    }

    /** @return the minimum required number of bytes to hold all data in this message */
    public int requiredRawSize() {
        return size;
    }

    public SubBytesStorage payload() {
        return subStorage(header.getSize(), size);
    }
    public int getPayloadLength() {
        return size-header.getSize();
    }


    /**
     * Creates a receipt for the given message, to be send back to the sender of the given message.
     * Contains the content hash(without the sender, since that was automatically attached on this side) of the given message.
     * The content hash can be validated using {@link #validateIsReceiptFor(P2LMessage)}.
     * @return a receipt for the given message, in p2l message form
     */
    public P2LMessage createReceipt() {
        if(header.isLongPart() || header.isStreamPart()) throw new UnsupportedOperationException("receipt cannot be created for parts - that would be like tcp ACK, but we wants receipts for many parts and this requires a difference functionality");
        //todo - use less cryptographic function - the checksum of udp is already pretty safe - so even without the hash at all it is pretty safe
        Hash receiptHash = getContentHash();
        P2LMessageHeader receiptHeader = new ReceiptHeader(header.getType(), header.getConversationId(), header.getStep());
        return receiptHeader.generateMessage(receiptHash.raw());
    }
    public boolean validateIsReceiptFor(P2LMessage message) {
        if(!header.isReceipt()) throw new IllegalStateException("cannot validate receipt, since this is not a receipt");
        Hash receiptHash = message.getContentHash();
        return header.getType() == message.header.getType() && header.getConversationId() == message.header.getConversationId() &&
                payloadEquals(receiptHash.raw());
    }

    /**
     * Efficient comparison of the payload within this message to the given array.
     * Does the comparison without decoding the payload or caching a copy of it.
     * @param o a byte array to be compared to the payload
     * @return whether the payload and the given array are equal
     */
    public boolean payloadEquals(byte[] o) {
        return payloadEquals(o, 0, o.length);
    }
    public boolean payloadEquals(byte[] o_raw, int o_header_size, int o_payloadLength) {
        int payloadLength = getPayloadLength();
        if(payloadLength != o_payloadLength) return false;
        for(int i=0;i<payloadLength;i++)
            if(content[header.getSize() + i] != o_raw[o_header_size + i])
                return false;
        return true;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LMessage that = (P2LMessage) o;
        return header.equals(that.header) && payloadEquals(that.content, that.header.getSize(), that.getPayloadLength());
    }
    @Override public int hashCode() {
        return header.hashCode() + 13*super.hashCode();
    }
    @Override public String toString() {
        return "P2LMessage{header=" + header + ", contentHash=" + contentHash + ", raw(pay=["+header.getSize()+", "+(size)+"])=" + Arrays.toString(Arrays.copyOfRange(content,0, header.getSize()+Math.min(getPayloadLength(), 12))) + '}';
    }





    //HEADER WRAPPER::

    /**
     * @return whether this message is internal or used by the application running on the p2l network
     */
    public boolean isInternalMessage() {
        return isInternalMessageId(header.getType());
    }

    /**
     * Mutates the header bytes of this message to request a receipt (used to simplify the factory methods)
     * MUTATES RAW!! (does not mutate contentHash though)
     * SHOULD ONLY BE USED INTERNALLY
     */
    public void mutateToRequestReceipt() {
        header.mutateToRequestReceipt(content);
    }
    public boolean isExpired() {
        return header.isExpired();
    }

    public static P2LMessage messagePartFrom(P2LMessage message, int index, int size, int from, int to) {
        int subPayloadLength = to-from;
        P2LMessageHeader partHeader = message.header.toMessagePartHeader(index, size);


        byte[] raw = partHeader.generateRaw(subPayloadLength);
        System.arraycopy(message.content, from, raw, partHeader.getSize(), subPayloadLength);
        return new P2LMessage(partHeader, null, raw, subPayloadLength);
    }

    public static ReceivedP2LMessage reassembleFromParts(ReceivedP2LMessage[] parts, int totalByteSize) {
        P2LMessageHeader reassembledHeader = parts[0].header.toShortMessageHeader();

        byte[] raw = reassembledHeader.generateRaw(totalByteSize);
        int raw_i = reassembledHeader.getSize();
        for(P2LMessage part:parts) {
            System.arraycopy(part.content, part.header.getSize(), raw, raw_i, part.getPayloadLength());
            raw_i+=part.getPayloadLength();
        }
        return new ReceivedP2LMessage(parts[0].sender, reassembledHeader, null, raw, totalByteSize);
    }

    public static P2LMessage createNatHolePacket() {
        P2LMessageHeader header = new MinimalHeader(SL_NAT_HOLE_PACKET, false);
        return header.generateMessage(new byte[0]);
    }

    public static P2LMessage from(P2LMessageHeader header, MessageEncoder encoded) {
        if(header.getSize() != encoded.offset) throw new IllegalArgumentException("given encoded message has an incorrect offset(!= header size) - ((used convo? try convo.encoder()))");
        header.writeTo(encoded.content, 0);
        return new P2LMessage(header, null, encoded.content, encoded.size-encoded.offset);
    }

    
    


    //USER LEVEL SEND MESSAGE GENERATION - ONLY PREVIOUSLY USED AND VERY BROAD ONES
    /**Not part of a conversation and expires instantly.*/
    public static P2LMessage with(int type) {
        return with(type, NO_CONVERSATION_ID, EXPIRE_INSTANTLY, false, 0);
    }
    public static P2LMessage with(int type, int conversationId, int expiration, boolean requestReceipt, int payloadLength) {
        return P2LMessageHeader.from(toShort(type), toShort(conversationId), toShort(expiration), NO_STEP, requestReceipt).generateMessage(payloadLength);
    }
    public static P2LMessage with(int type, byte[] payload) {
        return with(type, NO_CONVERSATION_ID, EXPIRE_INSTANTLY, false, payload);
    }
    public static P2LMessage withExpiration(int type, int expiration, byte[] payload) {
        return with(type, NO_CONVERSATION_ID, expiration, false, payload);
    }
    public static P2LMessage with(int type, int conversationId, int expiration, boolean requestReceipt, byte[] payload) {
        return P2LMessageHeader.from(toShort(type), toShort(conversationId), toShort(expiration), NO_STEP, requestReceipt).generateMessage(payload);
    }
}
