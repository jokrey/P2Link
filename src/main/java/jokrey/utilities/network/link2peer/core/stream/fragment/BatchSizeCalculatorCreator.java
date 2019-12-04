package jokrey.utilities.network.link2peer.core.stream.fragment;

import jokrey.utilities.network.link2peer.core.P2LConnection;

/**
 * @author jokrey
 */
public interface BatchSizeCalculatorCreator {
    BatchSizeCalculator create(P2LConnection connection);
}
