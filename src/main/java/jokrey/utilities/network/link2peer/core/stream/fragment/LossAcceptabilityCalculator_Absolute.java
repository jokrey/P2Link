package jokrey.utilities.network.link2peer.core.stream.fragment;

/**
 * @author jokrey
 */
public class LossAcceptabilityCalculator_Absolute implements LossAcceptabilityCalculator {
    public static int EXCESSIVE_LOSS_THRESHOLD = 50_000;

    private final int acceptableNumBytesLost;
    public LossAcceptabilityCalculator_Absolute(int acceptableNumBytesLost) {
        this.acceptableNumBytesLost = acceptableNumBytesLost;
    }

    @Override public LossResult calculateAcceptability(long numRecentlyLostBytes, long numRecentlySentBytes, int packageSize) {
        if(numRecentlyLostBytes == 0)
            return LossResult.NO_LOSS;
        if(numRecentlyLostBytes < acceptableNumBytesLost)
            return LossResult.ACCEPTABLE_LOSS;
        if(numRecentlyLostBytes > EXCESSIVE_LOSS_THRESHOLD)
            return LossResult.EXCESSIVE_LOSS;
        return LossResult.UNACCEPTABLE_LOSS;
    }
}
