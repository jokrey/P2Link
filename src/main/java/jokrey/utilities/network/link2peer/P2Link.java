package jokrey.utilities.network.link2peer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Link requirements
 *       String representations
 *              For broadcast messages
 *              For relaying connections to other peers so they may establish connections to it
 *              For establishing connections
 *           String representation must:
 *              Allow dns and ip addresses
 *              Contain ports
 *              Allow being 'light peer links' - i.e. links to a relay server with an id
 *                    Hidden links are automatically created when a hidden node(i.e. a node that does not know its own public ip) connects
 *
 * @author jokrey
 */
public class P2Link {
    private String stringRepresentation;
    private final InetSocketAddress rawAddr;
    private final int port;
    private final P2Link relayServerLink;

    private P2Link(String stringRepresentation, InetSocketAddress rawAddr, P2Link relayServerLink, int port) {
//        if(rawAddr!=null && rawAddr.getAddress().getCanonicalHostName().equals("localhost"))
//            rawAddr=null;
        this.stringRepresentation = stringRepresentation;
        this.rawAddr = rawAddr;
        this.port = port;
        this.relayServerLink = relayServerLink;

        //todo
//        if(relayServerLink!=null && relayServerLink.getSocketAddress() == null)
//            throw new IllegalArgumentException("relay server link cannot be hidden");
    }

    public static P2Link fromStringEnsureRelayLinkAvailable(String raw, InetSocketAddress to) {
        P2Link link = fromString(raw);
        if(link.isHiddenLink() && !link.relayServerLink.isDirectLink())
            return new P2Link(null, link.getSocketAddress(), P2Link.createDirectLink(to), link.getPort());
        return link;
    }

    public int getPort() {
        return port;
    }
    public InetSocketAddress getSocketAddress() { return rawAddr; }
    public SocketAddress getRelaySocketAddress() {
        return relayServerLink.getSocketAddress();
    }
    public P2Link getRelayLink() {
        return relayServerLink;
    }

    public boolean isDirectLink() {
        return relayServerLink ==null && rawAddr != null;
    }
    public boolean isHiddenLink() {
        return relayServerLink != null && rawAddr != null;
    }
    public boolean isLocalLink() {
        return rawAddr == null;
    }

    /** Resolves local link to the localhost:port combination. NOTE: The resulting link will return 'isPublicLink' == true */
    public P2Link toDirect() {return new P2Link(null, new InetSocketAddress("localhost", port), relayServerLink, port);}

    public P2Link toHidden(P2Link relayLink) {
        if(isLocalLink()) return new P2Link(null, new InetSocketAddress("localhost", port), relayLink, port);
        if(isHiddenLink()) return new P2Link(null, rawAddr, relayLink, port); //overwrite existing relay link
        if(isDirectLink()) return new P2Link(null, rawAddr, relayLink, port);
        throw new IllegalStateException("impossible - cannot be not local, hidden and direct at the same time");
    }

    public String getStringRepresentation() {
        //todo
        if(stringRepresentation==null) {
            if(rawAddr==null) //private/local/unknown link
                stringRepresentation = Integer.toString(port);
            else if(relayServerLink ==null) //public link
                stringRepresentation = rawAddr.getAddress().getHostName() + ":" + rawAddr.getPort();
            else {//hidden link
                stringRepresentation = rawAddr.getAddress().getHostName() + ":" + rawAddr.getPort() + "(relay="+relayServerLink.getStringRepresentation()+")";
            }
        }
        return stringRepresentation;
    }
    public byte[] getBytesRepresentation() {
        return getStringRepresentation().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * @param stringRepresentation of the form: <port> || <ip/dns>:<port> || <ip/dns>:<port>(relay=<ip/dns>:<port>)
     * @return p2link or throws error
     */
    public static P2Link fromString(String stringRepresentation) {
        if(stringRepresentation.contains("(")) { //hidden
            String[] splitOuter = stringRepresentation.split("\\(relay=");
            String[] split = splitOuter[0].split(":");
            return P2Link.createHiddenLink(fromString(splitOuter[1].replace(")","")), new InetSocketAddress(split[0], Integer.parseInt(split[1])));
        } else if(stringRepresentation.contains(":")) {//public, because already not hidden
            String[] split = stringRepresentation.split(":");
            return P2Link.createDirectLink(split[0], Integer.parseInt(split[1]));
        } else {
            return P2Link.createLocalLink(Integer.parseInt(stringRepresentation));
        }
    }
    public static P2Link fromBytes(byte[] asBytes) {
        return fromString(new String(asBytes, StandardCharsets.UTF_8));
    }






    public static P2Link createDirectLink(String publicIpOrDns, int port) {
        return new P2Link(publicIpOrDns+":"+port, new InetSocketAddress(publicIpOrDns, port), null, port);
    }
    public static P2Link createDirectLink(InetSocketAddress socketAddress) {
        return new P2Link(null, socketAddress, null, socketAddress.getPort());
    }
    public static P2Link raw(SocketAddress socketAddress) {
        return createDirectLink((InetSocketAddress) socketAddress);
    }

    public static P2Link createHiddenLink(P2Link relayServerLink, InetSocketAddress naiveAddress) {
        return new P2Link(null, naiveAddress, relayServerLink, naiveAddress.getPort());
    }

    public static P2Link createLocalLink(int port) {
        return new P2Link( port+"", null, null, port);
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
