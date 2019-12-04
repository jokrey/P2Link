package jokrey.utilities.network.link2peer.core.stream.fragment;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface LossAcceptabilityCalculator {
    LossResult calculateAcceptability(long numRecentlyLostBytes, long numRecentlySentBytes, int packageSize);
}
