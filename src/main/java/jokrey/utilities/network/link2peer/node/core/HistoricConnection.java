package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;

/**
 * @author jokrey
 */
class HistoricConnection extends P2LConnection {
    public HistoricConnection(P2Link link, int remoteBufferSize, int avRTT) {
        super(link, remoteBufferSize, avRTT);
        if(link.getSocketAddress()==null)
            throw new NullPointerException("otherwise we would have a problem on retry");
    }

    long nextAttemptAt = System.currentTimeMillis();
    int numberOfAttemptsMade;

    boolean retryNow(long now) {
        if(nextAttemptAt <= now) {
            long newTime = (long) (nextAttemptAt + P2LHeuristics.ORIGINAL_RETRY_HISTORIC_TIMEOUT_MS * Math.pow(2, numberOfAttemptsMade));
            if (newTime < nextAttemptAt) {
                nextAttemptAt = Long.MAX_VALUE;
            } else {
                nextAttemptAt = newTime;
                numberOfAttemptsMade++;
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "HistoricConnection{" +
                "nextAttemptAt=" + nextAttemptAt +
                ", numberOfAttemptsMade=" + numberOfAttemptsMade +
                ", link=" + link +
                ", remoteBufferSize=" + remoteBufferSize +
                ", avRTT=" + avRTT +
                ", lastPacketReceived=" + lastPacketReceived +
                '}';
    }
}