package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.network.link2peer.P2LMessage;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Hash of a message.
 *
 * @author jokrey
 */
public class Hash {
    private final byte[] backingArray;
    public Hash(byte[] backingArray) {
        this.backingArray = backingArray;
    }

    public static Hash contentHashFrom(String sender, byte[] raw, int payloadLength) {
        //fixme speed ok?
        try {
            MessageDigest hashFunction = MessageDigest.getInstance("SHA-1");
            if(sender!=null)
                hashFunction.update(sender.getBytes(StandardCharsets.UTF_8));
            hashFunction.update(raw, P2LMessage.HEADER_TYPE_BYTES_OFFSET_INDEX, 4); //type
            hashFunction.update(raw, P2LMessage.HEADER_CONVERSATION_ID_BYTES_OFFSET_INDEX, 4); //conversation id
            if(payloadLength > 0)
                hashFunction.update(raw, P2LMessage.HEADER_SIZE, payloadLength); //only payload
            return new Hash(hashFunction.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new Error("missing critical algorithm");
        }
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Arrays.equals(backingArray, ((Hash) o).backingArray);
    }
    @Override public int hashCode() { return Arrays.hashCode(backingArray); }
    @Override public String toString() { return "Hash{backingArray=" + Arrays.toString(backingArray) + '}'; }
    public int length() {
        return 20;
    }

    public byte[] raw() { return backingArray; }
}