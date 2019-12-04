package jokrey.utilities.network.link2peer.core.stream.fragment;

/**
 * @author jokrey
 */
public class BatchSizeCalculator_Static extends BatchSizeCalculator {
    public static BatchSizeCalculatorCreator DEFAULT_CREATOR = c -> new BatchSizeCalculator_Static(256);

    private final int bs;
    public BatchSizeCalculator_Static(int size) {
        super(null);
        bs = size;
    }

    @Override public int getBatchSize() { return bs; }
    @Override public void adjustBatchSize(LossResult lossResult) { }
}
