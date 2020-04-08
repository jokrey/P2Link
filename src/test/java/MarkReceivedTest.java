import jokrey.utilities.network.link2peer.node.stream.P2LFragmentInputStreamImplV1;
import jokrey.utilities.simple.data_structure.lists.LongTupleList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author jokrey
 */
public class MarkReceivedTest {
    @Test void missingExactlyReceived() {
        LongTupleList missingRanges = new LongTupleList(0, 101);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 0, 101);
        assertEquals(new LongTupleList( /* empty */ ), missingRanges);
        assertEquals(136, newHigh);
    }
    @Test void receivedContainedInAMissingRange() {
        LongTupleList missingRanges = new LongTupleList(0, 100);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 50, 80);
        assertEquals(new LongTupleList(0, 50, 80, 100 ), missingRanges);
        assertEquals(136, newHigh);
    }

    @Test void missingEntirelyContained_startEquals() {
        LongTupleList missingRanges = new LongTupleList(0, 101);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 0, 136);
        assertEquals(new LongTupleList( /* empty */ ), missingRanges);
        assertEquals(136, newHigh);
    }
    @Test void twoMissingEntirelyContained_startEquals() {
        LongTupleList missingRanges = new LongTupleList(0, 101, 102, 106);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 0, 136);
        assertEquals(new LongTupleList( /* empty */ ), missingRanges);
        assertEquals(136, newHigh);
    }
    @Test void missingEntirelyContained() {
        LongTupleList missingRanges = new LongTupleList(55, 101);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 0, 136);
        assertEquals(new LongTupleList( /* empty */ ), missingRanges);
        assertEquals(136, newHigh);
    }
    @Test void twoMissingEntirelyContained() {
        LongTupleList missingRanges = new LongTupleList(55, 101, 120, 131);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 0, 136);
        assertEquals(new LongTupleList( /* empty */ ), missingRanges);
        assertEquals(136, newHigh);
    }
    @Test void manyMissingEntirelyContained() {
        LongTupleList missingRanges = new LongTupleList(0, 99, 101, 199, 201, 299, 301, 399, 401, 599, 601, 799);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 0, 1000);
        assertEquals(new LongTupleList( /* empty */ ), missingRanges);
        assertEquals(1000, newHigh);
    }
    @Test void missingRangePartiallyContained_greaterStart() {
        LongTupleList missingRanges = new LongTupleList(0, 100);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(136, missingRanges, 50, 1000);
        assertEquals(new LongTupleList(0, 50 ), missingRanges);
        assertEquals(1000, newHigh);
    }
    @Test void missingRangePartiallyContained_smallerEnd() {
        LongTupleList missingRanges = new LongTupleList(950, 1050);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(1050, missingRanges, 50, 1000);
        assertEquals(new LongTupleList(1000, 1050 ), missingRanges);
        assertEquals(1050, newHigh);
    }
    @Test void missingRangePartiallyContained_greaterStartAndSmallerEnd() {
        LongTupleList missingRanges = new LongTupleList(0, 100, 950, 1050);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(1050, missingRanges, 50, 1000);
        assertEquals(new LongTupleList(0, 50, 1000, 1050 ), missingRanges);
        assertEquals(1050, newHigh);
    }
    @Test void missingRangePartiallyContainedAndManyEntirelyContained_greaterStartAndSmallerEnd() {
        LongTupleList missingRanges = new LongTupleList(0, 100, 150, 200, 300, 400, 500, 600, 950, 1050);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(1050, missingRanges, 50, 1000);
        assertEquals(new LongTupleList(0, 50, 1000, 1050 ), missingRanges);
        assertEquals(1050, newHigh);
    }

    @Test void rangesPartiallyContainEachOther() {
        LongTupleList missingRanges = new LongTupleList(50, 129);
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(1050, missingRanges, 119, 129);
        assertEquals(new LongTupleList(50, 119 ), missingRanges);
        assertEquals(1050, newHigh);
    }

    @Test void t6() {
        LongTupleList missingRanges = new LongTupleList();
        long newHigh = P2LFragmentInputStreamImplV1.markReceived(36, missingRanges, 0, 136);
        assertEquals(new LongTupleList(), missingRanges);
        assertEquals(136, newHigh);
    }
    @Test void t7() {

    }
    @Test void t8() {

    }
    @Test void t9() {

    }
}
