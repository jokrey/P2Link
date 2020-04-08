package jokrey.utilities.network.link2peer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Only to be used by higher level operations
 *    such as user operations, connect or others
 *    NOT to be used by low level constructs (those should use the resolved, raw socket addresses)
 *
 * @author jokrey
 */
public abstract class P2Link {
    // technically it is not possible to distinguish a public link from a link behind a nat or only local
    //   we could do heuristics, but we can also not do that.
//    public abstract boolean isPublic();
    public abstract boolean isDirect();
    public abstract boolean isRelayed();
    public abstract boolean isOnlyLocal();

    public abstract int getPort();


    /**
     * {@code
     * Format:
     *   direct link:
     *      <ip/dns>:<port>
     *   relayed link:
     *      <name>[<direct link of relay server>]
     *   local link:
     *      <name>[local=<port>]
     *}
     * if input is null, result is null
     * @throws RuntimeException if not correct (i.e. not created by this objects toString method) */
    public static P2Link from(String representation) {
        if(representation == null) return null;
        if(representation.contains("[local=")) {
            return Local.from(representation);
        } else if(representation.contains("[") && representation.contains(":")) {
            return Relayed.from(representation);
        } else {
            return Direct.from(representation);
        }
    }

    //todo - more efficient representation likely possible
    public static P2Link from(byte[] representation) {
        return from(new String(representation, StandardCharsets.UTF_8));
    }
    public byte[] toBytes() {
        return toString().getBytes(StandardCharsets.UTF_8);
    }



    public static class Direct extends P2Link {
        public final String dnsOrIp;
        public final int port;

        public Direct(String dnsOrIp, int port) {
            this.dnsOrIp = dnsOrIp;
            this.port = port;
        }
        public Direct(InetSocketAddress raw) {
            this(raw.getHostName(), raw.getPort());
        }

        @Override public boolean isDirect() { return true; }
        @Override public boolean isRelayed() { return false; }
        @Override public boolean isOnlyLocal() { return false; }


        /** @throws RuntimeException if not correct (i.e. not created by this objects toString method) */
        public static Direct from(String representation) {
            String[] split = representation.split(":");
            return new Direct(split[0], Integer.parseInt(split[1]));
        }
        @Override public String toString() {
            return dnsOrIp + ':' + port;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Direct direct = (Direct) o;
            return port == direct.port && Objects.equals(dnsOrIp, direct.dnsOrIp);
        }
        @Override public int hashCode() {
            return Objects.hash(dnsOrIp, port);
        }

        public InetSocketAddress resolve() {
            return new InetSocketAddress(dnsOrIp, port);
        }
        @Override public int getPort() { return port; }
    }


    public static abstract class Named extends P2Link {
        public final String name;

        public Named(String name) {
            this.name = name;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Named)) return false;
            Named named = (Named) o;
            return Objects.equals(name, named.name);
        }
        @Override public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Relayed extends Named {
        public final Direct relayLink;

        public Relayed(String name, Direct relayLink) {
            super(name);
            this.relayLink = relayLink;
        }

        @Override public boolean isDirect() {
            return false;
        }
        @Override public boolean isRelayed() {
            return true;
        }
        @Override public boolean isOnlyLocal() {
            return false;
        }

        /** @throws RuntimeException if not correct (i.e. not created by this objects toString method) */
        public static Relayed from(String representation) {
            String[] split = representation.split("\\[");
            return new Relayed(split[0], Direct.from(split[1].substring(0, split[1].length()-1)));
        }

        @Override public String toString() {
            return name + '[' + relayLink + ']';
        }

        @Override public int getPort() {
            throw new UnsupportedOperationException("Port is unknown for relayed connections (as it can be changed by intermediate NATs)");
        }
    }

    public static class Local extends Named {
        public final int port;

        public Local(String name, int port) {
            super(name);
            this.port = port;
        }
        public static Local forTest(int port) {
            return new Local(Integer.toString(port), port);
        }

        @Override public boolean isDirect() { return false; }
        @Override public boolean isRelayed() { return false; }
        @Override public boolean isOnlyLocal() { return true; }


        /** @throws RuntimeException if not correct (i.e. not created by this objects toString method) */
        public static Local from(String representation) {
            String[] split = representation.split("\\[local=");
            return new Local(split[0], Integer.parseInt(split[1].substring(0, split[1].length()-1)));
        }
        @Override public String toString() {
            return name + "[local=" + port+"]";
        }

        @Override public int getPort() { return port; }

        public P2Link withRelay(Direct direct) {
            return new P2Link.Relayed(name, direct);
        }
        public Direct unsafeAsDirect() {
            return new Direct("localhost", port);
        }
    }
}
