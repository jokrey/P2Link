package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2Link;

import java.util.Objects;

/**
 * @author jokrey
 */
public class P2LConnection {
    public final P2Link link;
    public final int remoteBufferSize;
    public int avRTT;
    public int fragmentStreamVar = -1;

    public P2LConnection(P2Link link, int remoteBufferSize, int avRTT) {
        this.link=link;
        this.remoteBufferSize = remoteBufferSize;
        this.avRTT = avRTT;
    }

    long lastPacketReceived = System.currentTimeMillis();
    boolean isDormant(long now) {
        return (now - lastPacketReceived) > P2LHeuristics.ESTABLISHED_CONNECTION_IS_DORMANT_THRESHOLD_MS;
    }
    void notifyActivity() {lastPacketReceived = System.currentTimeMillis();}


    @Override public String toString() {
        return "P2LConnection{" +
                "link=" + link +
                ", remoteBufferSize=" + remoteBufferSize +
                ", avRTT=" + avRTT +
                ", lastPacketReceived=" + lastPacketReceived +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2LConnection that = (P2LConnection) o;
        return remoteBufferSize == that.remoteBufferSize &&
                avRTT == that.avRTT &&
                Objects.equals(link, that.link);
    }
    @Override public int hashCode() {
        return Objects.hash(link, remoteBufferSize, avRTT);
    }
}