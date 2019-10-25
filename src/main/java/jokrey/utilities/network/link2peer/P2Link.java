package jokrey.utilities.network.link2peer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A P2Link represents a node. To it a connection can be established and messages can be send.
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
    public final String ipOrDnsOrLtId;
    /** port of the link - defaults to 52189, guaranteed to be between 0 and 2^16  */
    public final int port;

    public final boolean isPublicLink;

    /**
     * Creates a link from ip/dns and port to a public node
     * @param ipOrDnsOrLtId an ip or a dns resolvable address.
     * @param port has be between 0 and 2^16(otherwise and exception is thrown)
     */
    public P2Link(String ipOrDnsOrLtId, int port) {
        this(ipOrDnsOrLtId, port, true);
    }
    /**
     * Creates a link from ip/dns/lt_id and port
     * @param ipOrDnsOrLtId an ip or a dns resolvable address.
     * @param port has be between 0 and 2^16(otherwise and exception is thrown)
     * @param isPublicLink whether this represents a link to a public or light node
     */
    public P2Link(String ipOrDnsOrLtId, int port, boolean isPublicLink) {
        this.raw = (isPublicLink?"":"lt:")+ipOrDnsOrLtId+":"+port;
        rawBytes = raw.getBytes(StandardCharsets.UTF_8);
        this.isPublicLink=isPublicLink;
        this.ipOrDnsOrLtId = ipOrDnsOrLtId;
        this.port = port;
        if(port < 0 || port > Short.MAX_VALUE*2)
            throw new IllegalArgumentException("port out of bounds");
    }
    /**
     * Creates a link from its raw representation ([ip/dns]:[port]).
     * @param rawLink raw link representation of the form [ip/dns]:[port]
     */
    public P2Link(String rawLink) {
        this.raw = rawLink;
        rawBytes = raw.getBytes(StandardCharsets.UTF_8);

        String[] split = rawLink.split(":");
        if(split.length==1) {
            ipOrDnsOrLtId = rawLink;
            port = DEFAULT_PORT;
            isPublicLink=true;
        } else if(split.length==2) {
            ipOrDnsOrLtId = split[0];
            port = Integer.parseInt(split[1]);
            isPublicLink=true;
        } else {
            ipOrDnsOrLtId = split[1];
            port = Integer.parseInt(split[2]);
            isPublicLink=false;
        }
        if(port < 0 || port > Short.MAX_VALUE*2)
            throw new IllegalArgumentException("port("+port+") out of bounds");
        if(ipOrDnsOrLtId == null || ipOrDnsOrLtId.isEmpty())
            throw new IllegalArgumentException("ip or dns is null or empty and therefore invalid - rawLink: "+rawLink);
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
        return Objects.hash(raw, ipOrDnsOrLtId, port);
    }
    @Override public String toString() {
        return "P2Link{"+ raw + '}';
    }

    public boolean validateResolvesTo(InetSocketAddress from) {
        InetSocketAddress reResolved = new InetSocketAddress(ipOrDnsOrLtId, port);
        return reResolved.getPort() == from.getPort() && reResolved.getAddress().equals(from.getAddress());
    }
}
