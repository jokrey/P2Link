package jokrey.utilities.network.link2peer.node.stream.fragment;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface LossAcceptabilityCalculator {
    LossResult calculateAcceptability(long numRecentlyLostBytes, long numRecentlySentBytes, int packageSize);
}
