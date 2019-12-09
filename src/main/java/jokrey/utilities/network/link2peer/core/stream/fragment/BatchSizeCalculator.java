package jokrey.utilities.network.link2peer.core.stream.fragment;

import jokrey.utilities.network.link2peer.core.P2LConnection;

/**
 * @author jokrey
 */
public abstract class BatchSizeCalculator {
    protected final P2LConnection connection;
    protected BatchSizeCalculator(P2LConnection connection) {
        this.connection = connection;
    }

    public abstract int getBatchSize();
    public abstract void adjustBatchSize(LossResult lossResult);
    public abstract BatchSizeCalculatorCreator creator();
}
