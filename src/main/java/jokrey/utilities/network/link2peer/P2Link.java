package jokrey.utilities.network.link2peer;

import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * @author jokrey
 */
public class P2Link {
    private String stringRepresentation;
    private final InetSocketAddress rawAddr;
    private final int port;

    private P2Link(String stringRepresentation, InetSocketAddress rawAddr, int port) {
        this.stringRepresentation = stringRepresentation;
        this.rawAddr = rawAddr;
        this.port = port;
    }
    private P2Link(String stringRepresentation, InetSocketAddress rawAddr) {
        this(stringRepresentation, rawAddr, rawAddr.getPort());
    }
    private P2Link(InetSocketAddress rawAddr) {
        this(null, rawAddr);
//        this(WhoAmIProtocol.toString(rawAddr), rawAddr);
    }
    private P2Link(int port) {
        this(null, null, port);
//        this(WhoAmIProtocol.toString(rawAddr), rawAddr);
    }

    public int getPort() {
        return port;
    }

    public String getStringRepresentation() {
        if(stringRepresentation==null) {
            if(rawAddr==null)
                stringRepresentation = port+"";
            else
                stringRepresentation = WhoAmIProtocol.toString(rawAddr);
        }
        return stringRepresentation;
    }

    @Override public String toString() {
        return stringRepresentation;
    }

    public static P2Link fromString(String stringRepresentation) {
        return null;
    }

    public boolean isHiddenLink() {
        return false; //todo
    }

    public static P2Link createHiddenLink(P2Link relayServerLink) {
        return null;
    }

    public static P2Link createPrivateLink(int port) {
        return new P2Link(null, new InetSocketAddress("localhost", port), port);
    }


    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(stringRepresentation, ((P2Link) o).stringRepresentation);
    }
    @Override public int hashCode() {
        return Objects.hash(stringRepresentation);
    }

    public SocketAddress getSocketAddress() {
        return rawAddr;
    }
}
