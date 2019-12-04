package jokrey.utilities.network.link2peer.util;

import java.util.Arrays;
import java.util.Objects;

/**
 * List of ints without boxing.
 * @author jokrey
 */
public class IntList {
    private static final int DEFAULT_CAPACITY = 10;

    private int[] backingArray;
    private int size;

    public IntList(int[] from) {
        this(from, from.length);
    }
    public IntList(int[] from, int size) {
        backingArray = from;
        this.size = size;
    }
    public IntList(int capacity) {
        backingArray = new int[capacity];
        size = 0;
    }
    public IntList() {
        this(DEFAULT_CAPACITY);
    }


    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean contains(int o) {
        for (int i = 0; i < size; i++) if (backingArray[i] == o) return true;
        return false;
    }

//    public IntIterator iterator() {
//        return null;
//    }

    public int[] toArray() {
        int[] a = new int[size];
        System.arraycopy(backingArray, 0, a, 0, size);
        return a;
    }

    public int[] toArray(int[] a) {
        if(a.length != size) throw new IllegalArgumentException("given array != size");
        System.arraycopy(backingArray, 0, a, 0, size);
        return a;
    }

    public boolean add(int o) {
        grow(size + 1);
        backingArray[size++] = o;
        return true;
    }

    public boolean remove(int o) {
        int indexOf = indexOf(o);
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

    public int get(int index) {
        return backingArray[index];
    }

    public int set(int index, int element) {
        int oldValue = backingArray[index];
        backingArray[index] = element;
        return oldValue;
    }

    public void add(int index, int element) {
        grow(size + 1);
        System.arraycopy(backingArray, index, backingArray, index + 1,
                size - index);
        backingArray[index] = element;
        size++;
    }

    public void fastRemoveIndex(int index) {
        int numMoved = size - index - 1;
        if (numMoved > 0)
            System.arraycopy(backingArray, index+1, backingArray, index, numMoved);
        size--;
    }
    public int removeIndex(int index) {
        if(index < 0 || index >= size)
            throw new IndexOutOfBoundsException("index("+index+") < 0 || index("+index+") > size("+size+")");
        int oldValue = backingArray[index];
        int numMoved = size - index - 1;
        if (numMoved > 0)
            System.arraycopy(backingArray, index+1, backingArray, index, numMoved);
        size--;
        return oldValue;
    }

    public int indexOf(int o) {
        for (int i = 0; i < size; i++) if (backingArray[i] == o) return i;
        return -1;
    }

    public int lastIndexOf(int o) {
        for (int i = size - 1; i >= 0; i--) if (backingArray[i] == o) return i;
        return -1;
    }

//    public IntList subList(int fromIndex, int toIndex) {
//        return null;
//    }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntList that = (IntList) o;
        if(size != that.size) return false;
        for(int i=0;i<size;i++)
            if(backingArray[i] != that.backingArray[i])
                return false;
        return true;
    }
    @Override public int hashCode() {
        int result = Objects.hash(size);
        for (int i = 0; i < size; i++) {
            long element = backingArray[i];
            int elementHash = (int) (element ^ (element >>> 32));
            result = 31 * result + elementHash;
        }
        return result;
    }
    @Override public String toString() {
        int iMax = size-1;
        if (iMax == -1)
            return "[]";
        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(backingArray[i]);
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
}
