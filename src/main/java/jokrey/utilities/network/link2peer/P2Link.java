package jokrey.utilities.network.link2peer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A P2Link represents a node. To it a connection can be established and messages can be send.
 *
 * A socket address, except that it retains the dns link if there was one...
 *
 * @author jokrey
 */
public class P2Link {
    public static final int DEFAULT_PORT = 52189;

    /**
     * Raw link representation of the form: [dns/ip]:[port]
     */
    public final String raw;

    /** ip / dns / lt_id part of the link */
    public final String ipOrDns;
    public boolean isPublicLinkKnown() {return ipOrDns != null;}
    public P2Link attachPublicLink(String ipOrDns) {
        if(isPublicLinkKnown())
            throw new IllegalStateException("cannot attach public link, public link is already known..");
        return new P2Link(ipOrDns, port);
    }

    /** port of the link - defaults to 52189, guaranteed to be between 0 and 2^16  */
    public final int port;

    /**
     * Creates a link from ip/dns/lt_id and port
     * @param ipOrDns an ip or a dns resolvable address.
     * @param port has be between 0 and 2^16(otherwise and exception is thrown)
     */
    public P2Link(String ipOrDns, int port) {//finished switch to udp (missing new features)
        this.raw = ipOrDns+":"+port;
        rawBytes = raw.getBytes(StandardCharsets.UTF_8);
        this.ipOrDns = ipOrDns;
        this.port = port;
        if(port < 0 || port > Short.MAX_VALUE*2)
            throw new IllegalArgumentException("port out of bounds");
    }
    /**
     * Creates a link from its raw representation ([ip/dns]:[port]).
     * If the link is to be used for creating a new node and no public facing ip/dns address is known the port number is sufficient.
     * The node will fill the missing ip/dns itself when connecting to other nodes.
     *
     * @param rawLink raw link representation of the form [ip/dns]:[port]
     */
    public P2Link(String rawLink) {
        this.raw = rawLink;
        rawBytes = raw.getBytes(StandardCharsets.UTF_8);

        String[] split = rawLink.split(":");
        if (split.length ==2) {
            ipOrDns = split[0];
            port = Integer.parseInt(split[1]);
        } else {
            ipOrDns = null;
            port = Integer.parseInt(rawLink);
        }
        if(port < 0 || port > Short.MAX_VALUE*2)
            throw new IllegalArgumentException("port("+port+") out of bounds");
    }

    /**
     * Decodes the link from the return of {@link #getRepresentingByteArray()}.
     * @param rawLink raw link as bytes, utf8 encoded.
     */
    public P2Link(byte[] rawLink) {
        this(new String(rawLink, StandardCharsets.UTF_8));
    }

    private final byte[] rawBytes;
    /**
     * @return a cached byte representation of this link, decodable with the appropriate constructor
     */
    public byte[] getRepresentingByteArray() {
        return rawBytes;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        P2Link p2Link = (P2Link) o;
        return Objects.equals(raw, p2Link.raw);
    }
    @Override public int hashCode() {
        return Objects.hash(raw, ipOrDns, port);
    }
    @Override public String toString() {
        return "P2Link{" + raw + '}';
    }

    public boolean validateResolvesTo(InetSocketAddress from) {
        InetSocketAddress reResolved = new InetSocketAddress(ipOrDns, port);
        return reResolved.getPort() == from.getPort() && reResolved.getAddress().equals(from.getAddress());
    }
}
