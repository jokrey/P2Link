import jokrey.utilities.network.link2peer.util.IntList;
import jokrey.utilities.network.link2peer.util.LongTupleList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author jokrey
 */
public class PrimitiveListsTests {
    @Test void intListTest() {
        IntList list = new IntList();
        boolean suc;
        assertTrue(list.isEmpty());
        suc = list.add(1);
        assertTrue(suc);
        assertEquals(1, list.get(0));
        assertEquals(1, list.size());
        suc = list.remove(0);
        assertFalse(suc);
        assertEquals(1, list.get(0));
        assertEquals(1, list.size());
        suc = list.add(0);
        assertTrue(suc);
        assertEquals(0, list.get(1));
        assertEquals(2, list.size());
        suc = list.remove(0);
        assertTrue(suc);
        assertEquals(1, list.get(0));
        assertEquals(1, list.size());
    }

    @Test void longTupleListTest() {
        LongTupleList list = new LongTupleList();
        boolean suc;
        assertTrue(list.isEmpty());
        suc = list.add(1, 1);
        assertTrue(suc);
        assertArrayEquals(new long[] {1,1}, list.get(0));
        assertEquals(1, list.size());
        suc = list.remove(0, 0);
        assertFalse(suc);
        assertArrayEquals(new long[] {1,1}, list.get(0));
        assertEquals(1, list.size());
        suc = list.add(0, 0);
        assertTrue(suc);
        assertArrayEquals(new long[] {0,0}, list.get(1));
        assertEquals(2, list.size());
        suc = list.remove(0, 0);
        assertTrue(suc);
        assertArrayEquals(new long[] {1,1}, list.get(0));
        assertEquals(1, list.size());
    }

    @Test void longTupleListTest2() {
        IntList listX = new IntList();
        for(int i=0;i<12;i++) {
            listX.add(i);
        }
        System.out.println("list = " + listX);
        listX.removeIndex(3);
        System.out.println("list = " + listX);

        LongTupleList list = new LongTupleList();
        for(int i=0;i<12;i++) {
            list.add(i, i);
        }
        System.out.println("list = " + list);
        list.removeIndex(3);
        System.out.println("list = " + list);
        list.removeIndex(10);
        System.out.println("list = " + list);

        list.set(3, 17, 17);
        System.out.println("list = " + list);
        list.add(3, 16, 16);
        System.out.println("list = " + list);
    }

    @Test void removeRangeTest() {
        LongTupleList list = new LongTupleList(0,0, 1,1, 2,2, 3,3, 4,4, 5,5, 6,6, 7,7, 8,8, 9,9);

        System.out.println("list = " + list);
        list.removeRange(1, 5);
        System.out.println("list = " + list);
        list.removeRange(1, 5);
        System.out.println("list = " + list);
        list.removeRange(0, list.size());
        System.out.println("list = " + list);

        list = new LongTupleList(0,0, 1,1, 2,2, 3,3, 4,4, 5,5, 6,6, 7,7, 8,8, 9,9);
        System.out.println("list = " + list);
        list.removeRange(0, list.size());
        System.out.println("list = " + list);
    }
}
