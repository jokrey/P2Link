package jokrey.utilities.network.link2peer.core.stream.fragment;

import jokrey.utilities.network.link2peer.core.P2LConnection;

/**
 * @author jokrey
 */
public class BatchSizeCalculator_DynamicBMS extends BatchSizeCalculator {
    public static BatchSizeCalculatorCreator DEFAULT_CREATOR = c -> new BatchSizeCalculator_DynamicBMS(c, 256);

    private float bsm = 2;
    private int bs;
    private int bs_last = bs;
    public BatchSizeCalculator_DynamicBMS(P2LConnection connection, int initialBs) {
        super(connection);
        bs = Math.max(connection.fragmentStreamVar, initialBs);
        bs = initialBs;
    }

    @Override public int getBatchSize() {
        return bs;
    }

    @Override public void adjustBatchSize(LossResult lossResult) {
        switch (lossResult) {
            case NO_LOSS:
                connection.fragmentStreamVar = bs;
                bs*=bsm;
                bsm+=0.01;
                break;
            case ACCEPTABLE_LOSS:
                bs*=bsm / 2;
                bsm+=0.005;
                break;
            case UNACCEPTABLE_LOSS:
            case EXCESSIVE_LOSS:
                if(bs == bs_last)
                    bs /= 2;
                else
                    bs = bs_last;
                bsm=1.01f;
                break;
        }
        bs_last = bs;
    }
}
