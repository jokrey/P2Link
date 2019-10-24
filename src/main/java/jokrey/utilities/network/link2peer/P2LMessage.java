package jokrey.utilities.network.link2peer;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.encoder.tag_based.implementation.paired.length_indicator.type.transformer.LITypeToBytesTransformer;
import jokrey.utilities.encoder.type_transformer.bytes.TypeToBytesTransformer;
import jokrey.utilities.network.link2peer.util.Hash;

import java.net.DatagramPacket;
import java.util.Arrays;
import java.util.Objects;

/**
 * A message always has a sender and data. What actual data is transported is naturally arbitrary.
 * A message also always comes with an type. Should the sender not specify an type, then the type will be at the default value of 0.
 * For non-internal user messages, the type will always be a positive integer - as negative type's are reserved for internal usage (such as ping and broadcast conversations).
 *
 * @author jokrey
 */
public class P2LMessage {
    /**
     * Sender of the message
     * for individual messages this will be the peer the message was received from
     * for broadcast messages this will be the peer that originally began distributing the message
     */
    public final P2Link sender;
    /**
     * Type of the message. A shortcut for applications to determine what this message represents without decoding the data field.
     * Also used to wait-for/expect certain types of messages and assigning custom handlers.
     */
    public final int type;
    /**
     * The data field used to communicate arbitrary data between peers.
     */
    public final byte[] data; //makes this class not immutable, which is why data should not be mutated

    private Hash hash;
    /**
     * @return a cached version of the hash of this message. The hash is 20 bytes long(sha1), usable with hash map and includes sender, type and data.
     */
    public Hash getHash() {
        if(hash == null)
            hash = Hash.from(sender, type, data); //no need for thread safety, same value computed in worst case
        return hash;
    }

    /**
     * Create a new P2LMessage, generally used internally by the algorithm.
     * @param sender sender of the message
     * @param type type of the message
     * @param data data of the message
     */
    public P2LMessage(P2Link sender, int type, byte[] data) {
        this.sender = sender;
        this.type = type;
        this.data = data;
    }

    public DatagramPacket getPacket() {
        byte[] typeAndData = new byte[4 + data.length];
        BitHelper.writeInt32(typeAndData, 0, type);
        System.arraycopy(data, 0, typeAndData, 4, data.length);
        return new DatagramPacket(typeAndData, typeAndData.length);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LMessage that = (P2LMessage) o;
        return Objects.equals(sender, that.sender) && Arrays.equals(data, that.data) && type == that.type;
    }
    @Override public int hashCode() {
        return hash.hashCode();
    }
    @Override public String toString() {
        return "P2LMessage{sender=" + sender + ", type=" + type + ", data=" + Arrays.toString(data) + ", hash=" + hash + '}';
    }

    //HELPER
    public static final TypeToBytesTransformer trans = new LITypeToBytesTransformer();
    public boolean asBool() {
        return trans.detransform_boolean(data);
    }
    public static byte[] fromBool(boolean b) {
        return trans.transform(b);
    }
    public byte asByte() {
        return trans.detransform_byte(data);
    }
    public static byte[] fromByte(byte b) {
        return trans.transform(b);
    }
    public char asChar() {
        return trans.detransform_char(data);
    }
    public static byte[] fromChar(char c) {
        return trans.transform(c);
    }
    public int asInt() {
        return trans.detransform_int(data);
    }
    public static byte[] fromInt(int i) {
        return trans.transform(i);
    }
    public long asLong() {
        return trans.detransform_long(data);
    }
    public static byte[] fromLong(long l) {
        return trans.transform(l);
    }
    public float asFloat() {
        return trans.detransform_float(data);
    }
    public static byte[] fromFloat(float f) {
        return trans.transform(f);
    }
    public double asDouble() {
        return trans.detransform_double(data);
    }
    public static byte[] fromDouble(double d) {
        return trans.transform(d);
    }
    public String asString() {
        return trans.detransform_string(data);
    }
    public static byte[] fromString(String s) {
        return trans.transform(s);
    }
}
