package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.network.link2peer.P2Link;

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

    public static Hash from(P2Link sender, byte[] raw) {
        //fixme speed ok?
        try {
            MessageDigest hashFunction = MessageDigest.getInstance("SHA-1");
            if(sender!=null)
                hashFunction.update(sender.getRepresentingByteArray());
            hashFunction.update(raw);
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