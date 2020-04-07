package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * @author jokrey
 */
public class P2LConnection {
    public final P2Link link;
    public final InetSocketAddress address;
    public final int remoteBufferSize;
    public int avRTT;
    public int fragmentStreamVar = -1;

    public P2LConnection(P2Link link, InetSocketAddress address, int remoteBufferSize) {
        this(link, address, remoteBufferSize, -1);
    }
    public P2LConnection(P2Link link, InetSocketAddress address, int remoteBufferSize, int avRTT) {
        this.link=link;
        this.address=address;
        this.remoteBufferSize = remoteBufferSize;
        this.avRTT = avRTT;
    }

    long lastPacketReceived = System.currentTimeMillis();
    boolean isDormant(long now) {
        return (now - lastPacketReceived) > P2LHeuristics.ESTABLISHED_CONNECTION_IS_DORMANT_THRESHOLD_MS;
    }
    void notifyActivity() {lastPacketReceived = System.currentTimeMillis();}

    public void updateAvRTT(int latestTook) {
        if(avRTT == -1)
            avRTT = latestTook;
        else
            avRTT = (avRTT*5 + latestTook)/6; //todo - maybe use a not as idiotic system -
    }

    @Override public String toString() {
        return "P2LConnection{" +
                "link=" + link +
//                ", address=" + address +
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
                Objects.equals(link, that.link)
//                && Objects.equals(address, that.address)
                ;
    }
    @Override public int hashCode() {
        return Objects.hash(link,
//                address,
                remoteBufferSize, avRTT);
    }
}