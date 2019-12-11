package jokrey.utilities.network.link2peer.node.stream.fragment;

/**
 * @author jokrey
 */
public class LossAcceptabilityCalculator_Packages implements LossAcceptabilityCalculator {
    public static int EXCESSIVE_LOSS_THRESHOLD = 20;

    private final int acceptableNumPackagesLost;
    public LossAcceptabilityCalculator_Packages(int acceptableNumPackagesLost) {
        this.acceptableNumPackagesLost = acceptableNumPackagesLost;
    }

    @Override public LossResult calculateAcceptability(long numRecentlyLostBytes, long numRecentlySentBytes, int packageSize) {
        long numRecentlyLostPackages = numRecentlyLostBytes/packageSize;
        if(numRecentlyLostPackages == 0)
            return LossResult.NO_LOSS;
        if(numRecentlyLostPackages < acceptableNumPackagesLost)
            return LossResult.ACCEPTABLE_LOSS;
        if(numRecentlyLostPackages > EXCESSIVE_LOSS_THRESHOLD)
            return LossResult.EXCESSIVE_LOSS;
        return LossResult.UNACCEPTABLE_LOSS;
    }
}
