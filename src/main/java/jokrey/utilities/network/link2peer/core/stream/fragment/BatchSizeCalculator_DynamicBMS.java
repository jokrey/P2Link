package jokrey.utilities.network.link2peer.core.stream.fragment;

import jokrey.utilities.network.link2peer.core.P2LConnection;

/**
 * @author jokrey
 */
public class BatchSizeCalculator_DynamicBMS extends BatchSizeCalculator {
    private final float min_bsm;
    private final float bsm_summand_no_loss;
    private final float bsm_summand_little_loss;
    private float bsm;
    private int bs;
    private int bs_last = bs;
    public BatchSizeCalculator_DynamicBMS(int initialBs, float initialBsm, float min_bsm, float bsm_summand_no_loss, float bsm_summand_little_loss) {
        this(null, initialBs, initialBsm, min_bsm, bsm_summand_no_loss, bsm_summand_little_loss);
    }
    public BatchSizeCalculator_DynamicBMS(P2LConnection connection, int initialBs, float initialBsm, float min_bsm, float bsm_summand_no_loss, float bsm_summand_little_loss) {
        super(connection);
        if(connection==null) bs = initialBs;
        else if(connection.fragmentStreamVar < 16) bs = initialBs;
        else bs = connection.fragmentStreamVar;
        bsm = initialBsm;
        this.min_bsm = min_bsm;
        this.bsm_summand_no_loss = bsm_summand_no_loss;
        this.bsm_summand_little_loss = bsm_summand_little_loss;
    }

    @Override public BatchSizeCalculatorCreator creator() {
        return c -> new BatchSizeCalculator_DynamicBMS(c, bs, bsm, min_bsm, bsm_summand_no_loss, bsm_summand_little_loss);
    }

    @Override public int getBatchSize() {
        return bs;
    }
    @Override public void adjustBatchSize(LossResult lossResult) {
        switch (lossResult) {
            case NO_LOSS:
                connection.fragmentStreamVar = bs;
                bs*=bsm;
                bsm+=bsm_summand_no_loss;
                break;
            case ACCEPTABLE_LOSS:
                bs*=bsm / 2;
                bsm+=bsm_summand_little_loss;
                break;
            case UNACCEPTABLE_LOSS:
            case EXCESSIVE_LOSS:
                if(bs == bs_last)
                    bs /= 2;
                else
                    bs = bs_last;
                bsm=min_bsm;
                break;
        }
        bs_last = bs;
    }
}
