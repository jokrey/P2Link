package jokrey.utilities.network.link2peer.util;

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