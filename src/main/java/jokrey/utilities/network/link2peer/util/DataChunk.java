package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * honestly, this class should have been a java.nio.ByteBuffer all along
 *
 * @author jokrey
 */
public class DataChunk {
    public final byte[] data;
    public int offset;
    public int lastDataIndex;//not actually last data index, but rather last data index + 1?????
    public DataChunk(int capacity) {
        this(new byte[capacity], 0,0);
    }
    public DataChunk(P2LMessage from) {
        this(from.raw, from.header.getSize(), from.payloadLength);
    }
    public DataChunk(byte[] data, int offset, int dataLen) {
        this(data, offset, dataLen, false);
    }
    public DataChunk(byte[] data, int offset, int dataLen, boolean truncate) {
        if(truncate) {
            this.data = Arrays.copyOfRange(data, offset, offset+dataLen);
            this.offset=0;
            this.lastDataIndex = dataLen;
        } else {
            this.data = data;
            this.offset = offset;
            this.lastDataIndex = offset+dataLen;
        }

    }
    public int size() {
        return lastDataIndex-offset;
    }
    public boolean isEmpty() {
        return offset == lastDataIndex;
    }

    //default implementations are valid
//    @Override public boolean equals(Object obj) { return super.equals(obj); }
//    @Override public int hashCode() { return super.hashCode(); }

    @Override public String toString() {
        String stringData = "";
        if(offset>=0 && offset < lastDataIndex && lastDataIndex <= data.length)
            stringData = new String(data, offset, lastDataIndex-offset, StandardCharsets.UTF_8);
        return "DataChunk{" + "offset=" + offset + ", lastDataIndex=" + lastDataIndex + ", stringData="+stringData + ", data=" + Arrays.toString(Arrays.copyOfRange(data, Math.max(0, data.length-4), data.length)) + '}';
    }

    public void copyTo(byte[] b, int off, int numberOfBytesToMove) {
        if(numberOfBytesToMove > size())
            throw new IllegalArgumentException("attempt to transfer more than available bytes out of buffer");
        System.arraycopy(data, offset, b, off, numberOfBytesToMove);
    }
    public void moveTo(byte[] b, int off, int numberOfBytesToMove) {
        copyTo(b, off, numberOfBytesToMove);
        offset+=numberOfBytesToMove;
    }
    public byte moveSingleByte() {
        offset++;
        return data[offset];
    }

    public byte[] copyRange(int from, int to) {
        return Arrays.copyOfRange(data, from, to);
    }
    public byte[] copyFrom(int from) {
        return Arrays.copyOfRange(data, from, lastDataIndex);
    }

    public void copyAll(byte[] b, int bOff) {
        System.arraycopy(data, 0, b, bOff, lastDataIndex);
    }
    public void cloneInto(DataChunk chunk) {
        System.arraycopy(data, 0, chunk.data, 0, lastDataIndex);
        chunk.offset=offset;
        chunk.lastDataIndex=lastDataIndex;
    }

    public int capacity() {
        return data.length;
    }

    public void put(byte bByte) {
        data[lastDataIndex] = bByte;
        lastDataIndex++;
    }
    public void put(byte[] b, int off, int len) {
        System.arraycopy(b, off, data, lastDataIndex, len);
        lastDataIndex+=len;
    }

    public boolean isFull() {
        return lastDataIndex == data.length;
    }
}