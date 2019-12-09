package jokrey.utilities.network.link2peer.core.stream.fragment;

/**
 * @author jokrey
 */
public class BatchSizeCalculator_Static extends BatchSizeCalculator {
    private final int bs;
    public BatchSizeCalculator_Static(int size) {
        super(null);
        bs = size;
    }

    @Override public BatchSizeCalculatorCreator creator() {
        return c -> new BatchSizeCalculator_Static(bs);
    }
    @Override public int getBatchSize() { return bs; }
    @Override public void adjustBatchSize(LossResult lossResult) { }
}
