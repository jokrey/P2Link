package jokrey.utilities.network.link2peer.node.stream.fragment;

import jokrey.utilities.network.link2peer.node.core.P2LConnection;

/**
 * @author jokrey
 */
public interface BatchSizeCalculatorCreator {
    BatchSizeCalculator create(P2LConnection connection);
}
