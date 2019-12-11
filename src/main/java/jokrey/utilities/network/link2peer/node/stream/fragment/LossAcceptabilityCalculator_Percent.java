package jokrey.utilities.network.link2peer.node.stream.fragment;

/**
 * @author jokrey
 */
public class LossAcceptabilityCalculator_Percent implements LossAcceptabilityCalculator {
    public static float EXCESSIVE_LOSS_THRESHOLD_PERCENT = .6f;

    private final float acceptableLoss;
    public LossAcceptabilityCalculator_Percent(float acceptableLoss) {
        this.acceptableLoss = acceptableLoss;
    }

    @Override public LossResult calculateAcceptability(long numRecentlyLostBytes, long numRecentlySentBytes, int packageSize) {
        if(numRecentlyLostBytes == 0)
            return LossResult.NO_LOSS;
        float percentLost = numRecentlyLostBytes/(float)numRecentlySentBytes;
        if(percentLost < acceptableLoss)
            return  LossResult.ACCEPTABLE_LOSS;
        if(percentLost > EXCESSIVE_LOSS_THRESHOLD_PERCENT)
            return LossResult.EXCESSIVE_LOSS;
        return LossResult.UNACCEPTABLE_LOSS;
    }
}
