package jokrey.utilities.network.link2peer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * @author jokrey
 */
public class P2Link {
    private String stringRepresentation;
    private final InetSocketAddress rawAddr;
    private final int port;
    private final P2Link relayServerLink;

    private P2Link(InetSocketAddress rawAddr, P2Link relayServerLink, int port) {
//        if(rawAddr!=null && rawAddr.getAddress().getCanonicalHostName().equals("localhost"))
//            rawAddr=null;
        this.rawAddr = rawAddr;
        this.port = port;
        this.relayServerLink = relayServerLink;
    }

    public int getPort() {
        return port;
    }
    public InetSocketAddress getSocketAddress() {
        return rawAddr;
    }

    public boolean isPublicLink() {
        return relayServerLink ==null && rawAddr != null;
    }
    public boolean isHiddenLink() {
        return relayServerLink !=null && rawAddr != null;
    }
    public boolean isPrivateLink() {
        return rawAddr == null;
    }



    public String getStringRepresentation() {
        //todo
        if(stringRepresentation==null) {
            if(rawAddr==null) //private/local/unknown link
                stringRepresentation = port+"";
            else if(relayServerLink ==null) //public link
                stringRepresentation = rawAddr.getAddress().getCanonicalHostName() + ":" + rawAddr.getPort();
            else {//hidden link
                stringRepresentation = relayServerLink.getStringRepresentation()+" for ("+rawAddr.getAddress().getCanonicalHostName() + ":" + rawAddr.getPort()+")";
            }
        }
        return stringRepresentation;
    }
    public byte[] getBytesRepresentation() {
        return getStringRepresentation().getBytes(StandardCharsets.UTF_8);
    }
    public static P2Link fromString(String stringRepresentation) {
        if(stringRepresentation.contains("(")) { //hidden
            String[] splitOuter = stringRepresentation.split(" for \\(");
            String[] split = splitOuter[1].split(":");
            return P2Link.createHiddenLink(fromString(splitOuter[0]), new InetSocketAddress(split[0], Integer.parseInt(split[1].replace(")",""))));
        } else if(stringRepresentation.contains(":")) {//public, because already not hidden
            String[] split = stringRepresentation.split(":");
            return P2Link.createPublicLink(split[0], Integer.parseInt(split[1]));
        } else {
            return P2Link.createPrivateLink(Integer.parseInt(stringRepresentation));
        }
    }
    public static P2Link fromBytes(byte[] asBytes) {
        return fromString(new String(asBytes, StandardCharsets.UTF_8));
    }






    public static P2Link createPublicLink(String publicIpOrDns, int port) {
        return new P2Link(new InetSocketAddress(publicIpOrDns, port), null, port);
    }
    public static P2Link createPublicLink(InetSocketAddress socketAddress) {
        return new P2Link(socketAddress, null, socketAddress.getPort());
    }
    public static P2Link raw(SocketAddress socketAddress) {
        return createPublicLink((InetSocketAddress) socketAddress);
    }

    public static P2Link createHiddenLink(P2Link relayServerLink, InetSocketAddress naiveAddress) {
        return new P2Link(naiveAddress, relayServerLink, naiveAddress.getPort());
    }

    public static P2Link createPrivateLink(int port) {
        return new P2Link( null, null, port);
    }



    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(getStringRepresentation(), ((P2Link) o).getStringRepresentation());
    }
    @Override public int hashCode() {
        return Objects.hash(getStringRepresentation());
    }
    @Override public String toString() {
        return getStringRepresentation();
    }
}
