package jokrey.utilities.network.link2peer;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.encoder.as_union.li.LIPosition;
import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.encoder.tag_based.implementation.paired.length_indicator.type.transformer.LITypeToBytesTransformer;
import jokrey.utilities.encoder.type_transformer.bytes.TypeToBytesTransformer;
import jokrey.utilities.network.link2peer.core.message_headers.CustomExpirationHeader;
import jokrey.utilities.network.link2peer.core.message_headers.MinimalHeader;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.core.message_headers.ReceiptHeader;
import jokrey.utilities.network.link2peer.util.Hash;

import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.isInternalMessageId;

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
    public static int CUSTOM_RAW_SIZE_LIMIT = 1024;

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
    
    /**
     * The data field used to communicate arbitrary data between peers.
     */
    public final byte[] raw;

    /** number of bytes in the payload - not necessarily raw.length-HeaderSize, because raw can sometimes be a larger buffer */
    public final int payloadLength;

    private Hash contentHash;
    /** @return a cached version of the contentHash of this message. The contentHash is 20 bytes long(sha1), usable with contentHash map and includes sender, type and data. */
    public Hash getContentHash() {
        if(contentHash == null)
            contentHash = header.contentHashFrom(raw, payloadLength); //no need for thread safety, same value computed in worst case
        return contentHash;
    }

    private byte[] payload;
    /** decodes all payload bytes - the returned array is cached and will be returned when this method is called again - the returned array should NOT BE MUTATED */
    public byte[] asBytes() {
        if(payload == null)//no need for thread safety measures since the same value is calculated...
            payload = Arrays.copyOfRange(raw, header.getSize(), requiredRawSize());
        return payload;
    }

    /** Create a new P2LMessage */
    public P2LMessage(P2LMessageHeader header, Hash contentHash, byte[] raw, int payloadLength, byte[] payload) {
        this.header = header;
        this.raw = raw;
        this.payloadLength = payloadLength;
        this.payload = payload;
        this.contentHash = contentHash;

        resetReader();
    }

    /** @return a udp datagram packet from the internal data - it can be decoded on the receiver side using {@link #fromPacket(P2Link, DatagramPacket)}
     * @param to receiver of the created datagram packet*/
    public DatagramPacket toPacket(SocketAddress to) {
        int actualLength = requiredRawSize();

        int maxSize = getMaxPacketSize();
        byte[] actual = raw;
        if(raw.length > maxSize) {
            if(actualLength < maxSize) { //never gonna trigger, when message created with factory methods
                byte[] shrunk = new byte[actualLength];
                System.arraycopy(raw, 0, shrunk, 0, shrunk.length);
                actual = shrunk;
            }
        }
        if (actual.length > CUSTOM_RAW_SIZE_LIMIT) throw new IllegalArgumentException("total size of raw cannot exceed " + CUSTOM_RAW_SIZE_LIMIT + ", user set limit - size here is: " + raw.length + " - max payload size is: " + (CUSTOM_RAW_SIZE_LIMIT - header.getSize()));
        if (actual.length > MAX_UDP_PACKET_SIZE) throw new IllegalArgumentException("total size of a udp packet cannot exceed " + MAX_UDP_PACKET_SIZE + " - size here is: " + raw.length);
//        if(raw.length > 512)
//            System.err.println("message greater than 512 bytes - this can be considered inefficient because intermediate low level protocols might break it up - size here is: "+raw.length);
        return new DatagramPacket(actual, actualLength, to);
    }

    private static int getMaxPacketSize() {
        return Math.min(CUSTOM_RAW_SIZE_LIMIT, MAX_UDP_PACKET_SIZE);
    }

    /** @return Decodes a udp datagram packet into a p2l message */
    public static P2LMessage fromPacket(P2Link sender, DatagramPacket packet) {
        byte[] raw = packet.getData();

        P2LMessageHeader header = P2LMessageHeader.from(raw, sender);

        if(raw.length > packet.getLength()*2 && raw.length > 4096) //fixme heuristic
            raw =  Arrays.copyOfRange(packet.getData(), 0, packet.getLength());

        //contentHash and payload are only calculated if required... Preferably the 'as' methods should be used to extract data.
        return new P2LMessage(header, null, raw, packet.getLength()-header.getSize(), null);
    }

    /** @return whether a single packet will suffice to send the data this message contains */
    public boolean canBeSentInSinglePacket() {
        return requiredRawSize() <= getMaxPacketSize();
    }

    /** @return the minimum required number of bytes to hold all data in this message */
    public int requiredRawSize() {
        return header.getSize() + payloadLength;
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
        //todo     - interesting would be a hash id that allows getting two receipts for the same sender-type-conversationId simultaneously  (though we are quickly approaching overkill territory here)
        Hash receiptHash = header.contentHashFromIgnoreSender(raw, payloadLength);
        P2LMessageHeader receiptHeader = new ReceiptHeader(null, header.getType(), header.getConversationId());
        return receiptHeader.generateMessage(receiptHash.raw());
    }
    public boolean validateIsReceiptFor(P2LMessage message) {
        if(!header.isReceipt()) throw new IllegalStateException("cannot validate receipt, since this is not a receipt");
        Hash receiptHash = message.header.contentHashFromIgnoreSender(message.raw, message.payloadLength);
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
        if(payloadLength != o_payloadLength) return false;
        for(int i=0;i<payloadLength;i++)
            if(raw[header.getSize() + i] != o_raw[o_header_size + i])
                return false;
        return true;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LMessage that = (P2LMessage) o;
        return header.equals(that.header) && payloadEquals(that.raw, that.header.getSize(), that.payloadLength);
    }
    @Override public int hashCode() {
        return header.hashCode() + 13*Arrays.hashCode(raw);
    }
    @Override public String toString() {
        return "P2LMessage{header=" + header + ", contentHash=" + contentHash + ", raw(pay=["+header.getSize()+", "+(header.getSize()+payloadLength)+"])=" + Arrays.toString(Arrays.copyOfRange(raw,0, header.getSize()+Math.min(payloadLength, 12))) + '}';
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
        header.mutateToRequestReceipt(raw);
    }
    public boolean isExpired() {
        return header.isExpired();
    }


    /**
     * Factory for P2LMessages.
     * Since received messages are automatically decoded, this factory only provides public methods to create send messages.
     *
     * Some methods are able to automatically encode certain objects(among them the standard types). It does this using {@link LITypeToBytesTransformer}.
     * Additionally it can encode multiple sub-packets of arbitrary length. It does this using {@link LIbae} functionality.
     * Either encoding method can be comfortably decoded on the receiver side using the decoder methods(such as {@link #nextBool()}, {@link #nextVariable()}, {@link #nextVariableString()}).
     */
    public static class Factory {
        /**
         * Creates an empty message to be send(i.e. sender is not set and will be determined by the node automatically).
         * The resulting message does not have a payload. This is typically useful in messages used for synchronization or receive receipts.
         * @param type message type - used to efficiently distinguish messages, allowing applications to wait for them
         * @return the new message
         */
        public static P2LMessage createSendMessage(int type) {
            return createRawSendMessage(type, P2LNode.NO_CONVERSATION_ID, EXPIRE_INSTANTLY, 0);
        }
        public static P2LMessage createSendMessage(int type, int conversationId) {
            return createRawSendMessage(type, conversationId, EXPIRE_INSTANTLY, 0);
        }
        public static P2LMessage createSendMessage(int type, short expiresAfter) {
            return createRawSendMessage(type, P2LNode.NO_CONVERSATION_ID, expiresAfter, 0);
        }
        public static P2LMessage createSendMessage(int type, int conversationId, short expiresAfter) {
            return createRawSendMessage(type, conversationId, expiresAfter, 0);
        }

        /**
         * Creates a message to be send(i.e. sender is not set and will be determined by the node automatically).
         * @param type message type - used to efficiently distinguish messages, allowing applications to wait for them
         * @param payload the payload of the message. The payload can be anything, but it's size cannot not exceed {@link #CUSTOM_RAW_SIZE_LIMIT} - {@link P2LMessageHeader#getSize()}.
         * @return the new message
         */
        public static P2LMessage createSendMessage(int type, byte[] payload) {
            return createRawSendMessage(type, P2LNode.NO_CONVERSATION_ID, EXPIRE_INSTANTLY, payload.length, payload);
        }
        public static P2LMessage createSendMessage(int type, int conversationId, byte[] payload) {
            return createRawSendMessage(type, conversationId, EXPIRE_INSTANTLY, payload.length, payload);
        }
        public static P2LMessage createSendMessage(int type, short expiresAfter, byte[] payload) {
            return createRawSendMessage(type, P2LNode.NO_CONVERSATION_ID, expiresAfter, payload.length, payload);
        }
        public static P2LMessage createSendMessage(int type, int conversationId, short expiresAfter, byte[] payload) {
            return createRawSendMessage(type, conversationId, expiresAfter, payload.length, payload);
        }

        public static P2LMessage createRawSendMessage(int type, int conversationId, short expiresAfter, int totalPayloadSize, byte[]... payloads) {
            P2LMessageHeader header = P2LMessageHeader.from(null, type, conversationId, expiresAfter);
            byte[] raw = header.generateRaw(totalPayloadSize);
            int index = header.getSize();
            for(byte[] payload : payloads) {
                System.arraycopy(payload, 0, raw, index, payload.length);
                index+=payload.length;
            }
            return new P2LMessage(header, null, raw, totalPayloadSize, payloads.length==1?payloads[0]:null); //sender does not need to be set on send messages - it is automatically determined by the received from the ip header of the packet
        }


        public static <T>P2LMessage createSendMessage(int type, T payload) {
            return createSendMessage(type, trans.transform(payload));
        }
        public static <T>P2LMessage createSendMessage(int type, short expiresAfter, T payload) {
            return createSendMessage(type, expiresAfter, trans.transform(payload));
        }
        public static P2LMessage createSendMessageWith(int type, Object... payloads) {
            return createSendMessageFrom(type, P2LNode.NO_CONVERSATION_ID, payloads);
        }
        public static P2LMessage createSendMessageFrom(int type, int conversationId, Object... payloads) {
            return createSendMessageFrom(type, conversationId, EXPIRE_INSTANTLY, payloads);
        }
        public static P2LMessage createSendMessageFromWithExpiration(int type, short expiresAfter, Object... payloads) {
            return createSendMessageFrom(type, P2LNode.NO_CONVERSATION_ID, expiresAfter, payloads);
        }
        public static P2LMessage createSendMessageFrom(int type, int conversationId, short expiresAfter, Object... payloads) {
            byte[][] total = new byte[payloads.length][];
            int sizeCounter = 0;
            for(int i=0;i<payloads.length;i++) {
                total[i] = trans.transform(payloads[i]);
                sizeCounter+=total[i].length;
            }
            return createRawSendMessage(type, conversationId, expiresAfter, sizeCounter, total);
        }
        public static P2LMessage createSendMessageFromVariables(int type, Object... payloads) {
            return createSendMessageFromVariablesWithExpiration(type, EXPIRE_INSTANTLY, payloads);
        }
        public static P2LMessage createSendMessageFromVariablesWithExpiration(int type, short expiresAfter, Object... payloads) {
            return createSendMessageFromVariablesWithExpiration(type, P2LNode.NO_CONVERSATION_ID, expiresAfter, payloads);
        }
        public static P2LMessage createSendMessageFromVariablesWithConversationId(int type, int conversationId, Object... payloads) {
            return createSendMessageFromVariablesWithExpiration(type, conversationId, EXPIRE_INSTANTLY, payloads);
        }
        public static P2LMessage createSendMessageFromVariablesWithExpiration(int type, int conversationId, short expiresAfter, Object... payloads) {
            byte[][] total = new byte[payloads.length*2][];
            int sizeCounter = 0;
            for(int i=0;i<total.length;i+=2) {
                total[i+1] = trans.transform(payloads[i/2]);
                total[i] = makeVariableIndicatorFor(total[i+1].length);
                sizeCounter+=total[i].length + total[i+1].length;
            }
            return createRawSendMessage(type, conversationId, expiresAfter, sizeCounter, total);
        }
        public static P2LMessage createSendMessageFromVariables(int type, Collection payloads) {
            return createSendMessageFromVariables(type, EXPIRE_INSTANTLY, payloads);
        }
        public static P2LMessage createSendMessageFromVariables(int type, short expiresAfter, Collection payloads) {
            Iterator payloadsIterator = payloads.iterator();
            byte[][] total = new byte[payloads.size()*2][];
            int sizeCounter = 0;
            for(int i=0;i<total.length;i+=2) {
                total[i + 1] = trans.transform(payloadsIterator.next());
                total[i] = makeVariableIndicatorFor(total[i + 1].length);
                sizeCounter += total[i].length + total[i + 1].length;
            }
            return createRawSendMessage(type, P2LNode.NO_CONVERSATION_ID, expiresAfter, sizeCounter, total);
        }

        public static P2LMessage createBroadcast(P2Link sender, int brdMsgType, Object payload) {
            return createBroadcast(sender, brdMsgType, trans.transform(payload));
        }
        public static P2LMessage createBroadcast(P2Link sender, int brdMsgType, byte[] payload) {
            return new MinimalHeader(sender, brdMsgType, false).generateMessage(payload);
        }
        public static P2LMessage createBroadcast(P2Link sender, int brdMsgType, short expiresAfter, Object payload) {
            return createBroadcast(sender, brdMsgType, expiresAfter, trans.transform(payload));
        }
        public static P2LMessage createBroadcast(P2Link sender, int brdMsgType, short expiresAfter, byte[] payload) {
            return new CustomExpirationHeader(sender, brdMsgType, expiresAfter, false).generateMessage(payload);
        }



        public static P2LMessage messagePartFrom(P2LMessage message, int index, int size, int from, int to) {
            int subPayloadLength = to-from;
            P2LMessageHeader partHeader = message.header.toMessagePartHeader(index, size);


            byte[] raw = partHeader.generateRaw(subPayloadLength);
            System.arraycopy(message.raw, from, raw, partHeader.getSize(), subPayloadLength);
            return new P2LMessage(partHeader, null, raw, subPayloadLength, null);
        }

        public static P2LMessage reassembleFromParts(P2LMessage[] parts, int totalByteSize) {
            P2LMessageHeader reassembledHeader = parts[0].header.toShortMessageHeader();

            byte[] raw = reassembledHeader.generateRaw(totalByteSize);
            int raw_i = reassembledHeader.getSize();
            for(P2LMessage part:parts) {
                System.arraycopy(part.raw, part.header.getSize(), raw, raw_i, part.payloadLength);
                raw_i+=part.payloadLength;
            }
            return new P2LMessage(reassembledHeader, null, raw, totalByteSize, null);
        }
    }

    //DECODE HELPER
    /** Type transformer used internally for type transformations - when encoding standard types into payload parts with this transformer, the message can be efficiently decoded */
    public static final TypeToBytesTransformer trans = new LITypeToBytesTransformer();

    private int pointer=-1;
    /** resets the internal iterating pointer */
    public void resetReader() {pointer = header.getSize();}

    /** decodes the next payload byte as a boolean (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_boolean(byte[])}) */
    public boolean nextBool() {
        return raw[pointer++] == 1;
    }
    /** decodes the next payload byte as a byte (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_byte(byte[])}) */
    public byte nextByte() {
        return raw[pointer++];
    }
    /** decodes the next 2 payload bytes as an integer(32bit) (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_int(byte[])}) */
    public short nextShort() {
        int before = pointer;
        pointer+=2; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getInt16From(raw, before);
    }
    /** decodes the next 4 payload bytes as an integer(32bit) (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_int(byte[])}) */
    public int nextInt() {
        int before = pointer;
        pointer+=4; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getInt32From(raw, before);
    }
    /** decodes the next 8 payload bytes as an integer(64bit) (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_long(byte[])}) */
    public long nextLong() {
        int before = pointer;
        pointer+=8; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getIntFromNBytes(raw, before, 8);
    }
    /** decodes the next 4 payload bytes as a floating point number(32 bit) (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_float(byte[])} (byte[])}) */
    public float nextFloat() {
        int before = pointer;
        pointer+=4; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getFloat32From(raw, before);
    }
    /** decodes the next 8 payload bytes as a floating point number(64 bit) (same as, but more context efficient {@link LITypeToBytesTransformer#detransform_double(byte[])} (byte[])}) */
    public double nextDouble() {
        int before = pointer;
        pointer+=8; //temp maybe useless if this evaluates to pointer value before...
        return BitHelper.getFloat64From(raw, before);
    }
    /** decodes the next n payload bytes as bytes - uses length indicator functionality to determine n (same as, but more context efficient {@link LIbae#decode(LIPosition)}) */
    public byte[] nextVariable() {
        long[] li_bounds = LIbae.get_next_li_bounds(raw, pointer, pointer, requiredRawSize() - 1);
        if(li_bounds == null) return null;
        pointer = (int) li_bounds[1];
        return Arrays.copyOfRange(raw, (int) li_bounds[0], (int) li_bounds[1]);
    }
    /** decodes the next n payload bytes as a utf8 string - uses length indicator functionality to determine n (same as, but more context efficient {@link LIbae#decode(LIPosition)}) */
    public String nextVariableString() {
        long[] li_bounds = LIbae.get_next_li_bounds(raw, pointer, pointer, requiredRawSize() - 1);
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
        return new String(raw, header.getSize(), payloadLength, StandardCharsets.UTF_8);
    }
}
