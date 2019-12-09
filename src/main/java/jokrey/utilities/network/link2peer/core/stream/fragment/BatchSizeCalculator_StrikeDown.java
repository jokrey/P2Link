package jokrey.utilities.network.link2peer.core.stream.fragment;

import jokrey.utilities.network.link2peer.core.P2LConnection;

/**
 * Good if you roughly know the bandwidth (set that to initial bs = bandwidth/p-size)
 *
 * @author jokrey
 */
public class BatchSizeCalculator_StrikeDown extends BatchSizeCalculator {
    private final int min_bs;
    private final int step_length;
    private int bs;
    private int lastBsWithoutLoss;
    public BatchSizeCalculator_StrikeDown(int initialBs, int min_bs, int step_length) {
        this(null, initialBs, min_bs, step_length);
    }
    public BatchSizeCalculator_StrikeDown(P2LConnection connection, int initialBs, int min_bs, int step_length) {
        super(connection);
        if(connection==null || connection.fragmentStreamVar < min_bs)
            bs = initialBs;
        else
            bs = connection.fragmentStreamVar;
        this.min_bs = min_bs;
        lastBsWithoutLoss = min_bs;
        this.step_length = step_length;
    }

    @Override public BatchSizeCalculatorCreator creator() {
        return c -> new BatchSizeCalculator_StrikeDown(c, bs, min_bs, step_length);
    }

    @Override public int getBatchSize() {
        return bs;
    }
    @Override public void adjustBatchSize(LossResult lossResult) {
        switch (lossResult) {
            case NO_LOSS:
                if(connection!=null)
                    connection.fragmentStreamVar = bs;
                lastBsWithoutLoss = bs;
                bs += step_length;
                break;
            case ACCEPTABLE_LOSS:
                if(lastBsWithoutLoss==bs)
                    lastBsWithoutLoss-=step_length; //this is weird - and barely explainable - used so that excessive loss does not instantly strike down, but can take a step down (if that step is still bad - THEN: STRIKE DOWN!
                break;
            case UNACCEPTABLE_LOSS:
            case EXCESSIVE_LOSS:
                if(bs == lastBsWithoutLoss) //unacceptable loss with known no loss - assume drastic change in network -> STRIKE DOWN
                    bs = min_bs;
                else
                    bs = lastBsWithoutLoss;
//                if(bs < min_bs) bs = min_bs; // cannot occur
                break;
        }
    }
}
