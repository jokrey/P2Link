package jokrey.utilities.network.link2peer.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * List of long tuples without boxing.
 * @author jokrey
 */
public class LongTupleList {
    private static final int DEFAULT_CAPACITY = 10;

    private long[] backingArray;
    private int size;

    public LongTupleList(long... from) {
        this(from, from.length);
    }
    public LongTupleList(long[] from, int len) {
        backingArray = from;
        this.size = len/2;
    }
    public LongTupleList(int capacity) {
        backingArray = new long[capacity*2];
        size = 0;
    }
    public LongTupleList() {
        this(DEFAULT_CAPACITY);
    }
    

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

//    public boolean contains(long... o) {
//        return contains(o[0], o[1]);
//    }
    public boolean contains(long o1, long o2) {
        for (int i = 0; i < size*2; i+=2) {
            long l1 = backingArray[i];
            long l2 = backingArray[i+1];
            if (l1 == o1 && l2 == o2)
                return true;
        }
        return false;
    }

//    public IntIterator iterator() {
//        return null;
//    }

    public long[] toFlattenedArray() {
        long[] a = new long[size*2];
        System.arraycopy(backingArray, 0, a, 0, size*2);
        return a;
    }
    public long[] toFlattenedArray(long[] a) {
        if(a.length != size*2) throw new IllegalArgumentException("given array != size*2");
        System.arraycopy(backingArray, 0, a, 0, size*2);
        return a;
    }

    public boolean add(long o1, long o2) {
//        System.out.println("add - o1 = " + o1 + ", o2 = " + o2);
        grow(size*2 + 2);
        int convertedIndex = size*2;
        size++;
        backingArray[convertedIndex] = o1;
        backingArray[convertedIndex+1] = o2;
        return true;
    }

    public boolean remove(long o1, long o2) {
        int indexOf = indexOf(o1, o2);
        if(indexOf >= 0) {
            fastRemoveIndex(indexOf);
            return true;
        }
        return false;
    }

//    public boolean containsAll(IntList list) {
//        return false;
//    }
//
//    public boolean addAll(IntList list) {
//        return false;
//    }
//
//    public boolean addAll(int index, IntList list) {
//        return false;
//    }

    public void clear() {
        size=0;
    }

    public void get(int index, long[] tango) {
        int convertedIndex = index*2;
        tango[0] = backingArray[convertedIndex];
        tango[1] = backingArray[convertedIndex+1];
    }
    public long[] get(int index) {
        int convertedIndex = index*2;
        return new long[] {backingArray[convertedIndex], backingArray[convertedIndex+1]};
    }

    public long[] getSet(int index, long o1, long o2) {
        long[] oldValue = get(index);
        set(index, o1, o2);
        return oldValue;
    }
    public void set(int index, long o1, long o2) {
        int convertedIndex = index * 2;
        backingArray[convertedIndex] = o1;
        backingArray[convertedIndex + 1] = o2;
    }
    public void set0(int index, long o1) {
        int convertedIndex = index * 2;
        backingArray[convertedIndex] = o1;
    }
    public void set1(int index, long o2) {
        int convertedIndex = index * 2;
        backingArray[convertedIndex + 1] = o2;
    }

    public void add(int index, long o1, long o2) {
        grow(size*2 + 2);
        int convertedIndex = index*2;
        System.arraycopy(backingArray, convertedIndex, backingArray, convertedIndex + 2, size*2 - convertedIndex);
        backingArray[convertedIndex] = o1;
        backingArray[convertedIndex+1] = o2;
        size++;
    }

    public void fastRemoveIndex(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index("+index+") < 0 || index("+index+") >= size("+size+")");
        int convertedIndex = index*2;
        int numMoved = size*2 - convertedIndex - 2;
        if (numMoved > 0)
            System.arraycopy(backingArray, convertedIndex + 2, backingArray, convertedIndex, numMoved);
        size--;
    }
    public long[] removeIndex(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index("+index+") < 0 || index("+index+") > size("+size+")");
        long[] oldValue = get(index);
        int convertedIndex = index*2;
        int numMoved = size*2 - convertedIndex - 2;
        if (numMoved > 0)
            System.arraycopy(backingArray, convertedIndex + 2, backingArray, convertedIndex, numMoved);
        size--;
        return oldValue;
    }

    /**
     * @param from inclusive
     * @param to exclusive
     */
    public void removeRange(int from, int to) {
        if(from < 0 || to > size)
            throw new IndexOutOfBoundsException("from("+from+") < 0 || to("+to+") > size("+size+")");

        int convertedFrom = from*2;
        int convertedTo = to*2;
//        System.err.println("from = " + from);
//        System.err.println("to = " + to);
//        System.err.println("size = " + size);
//        System.err.println("convertedFrom = " + convertedFrom);
//        System.err.println("convertedTo = " + convertedTo);
//        System.err.println("((convertedTo + 2) - convertedFrom) = " + ((convertedTo + 2) - convertedFrom));
//        System.err.println("backingArray.length = " + backingArray.length);
        int numMoved = (convertedTo + 2) - convertedFrom;
        if(convertedTo+numMoved >= backingArray.length)
            numMoved = backingArray.length - convertedTo;
        if(convertedFrom+numMoved >= backingArray.length)
            numMoved = backingArray.length - convertedFrom;
        System.arraycopy(backingArray, convertedTo, backingArray, convertedFrom, numMoved);
        size -= to-from;
    }

    public int indexOf(long o1, long o2) {
        for (int i = 0; i < size*2; i+=2) if (backingArray[i] == o1 && backingArray[i+1] == o2) return i/2;
        return -1;
    }

    public int lastIndexOf(long o1, long o2) {
        for (int i = size*2 - 1; i >= 0; i-=2) if (backingArray[i] == o1 && backingArray[i+1] == o2) return i/2;
        return -1;
    }

//    public IntList subList(int fromIndex, int toIndex) {
//        return null;
//    }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LongTupleList that = (LongTupleList) o;
        if(size != that.size) return false;
        for(int i=0;i<size*2;i++)
            if(backingArray[i] != that.backingArray[i])
                return false;
        return true;
    }
    @Override public int hashCode() {
        int result = Objects.hash(size);
        for (int i = 0; i < size*2; i++) {
            long element = backingArray[i];
            int elementHash = (int) (element ^ (element >>> 32));
            result = 31 * result + elementHash;
        }
        return result;
    }
    @Override public String toString() {
        int iMax = size*2-2;
        if (iMax == -2)
            return "[]";
        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i+=2) {
            b.append("{").append(backingArray[i]).append(",").append(backingArray[i+1]).append("}");
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    //code adapted from java default arraylist implementation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    private void grow(int minCapacity) {
        int oldCapacity = backingArray.length;
        if(minCapacity <= oldCapacity)
            return;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);

        backingArray = Arrays.copyOf(backingArray, newCapacity);
    }
    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }

    public long get0(int index) {
        return backingArray[index*2];
    }
    public long get1(int index) {
        return backingArray[index*2+1];
    }
}
