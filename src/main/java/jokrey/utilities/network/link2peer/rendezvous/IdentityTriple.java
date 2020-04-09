package jokrey.utilities.network.link2peer.rendezvous;

import jokrey.utilities.encoder.as_union.li.bytes.MessageEncoder;
import jokrey.utilities.network.link2peer.P2Link;

import java.util.Arrays;
import java.util.Objects;

public class IdentityTriple {
    public final String name;
    public final byte[] publicKey;
    public final P2Link link;

    public IdentityTriple(String name, byte[] publicKey, P2Link link) {
        this.name = name;
        this.publicKey = publicKey;
        this.link = link;
    }

    public MessageEncoder encodeInto(MessageEncoder encoder, boolean includeLink) {
        encoder.encodeVariableString(name);
        encoder.encodeVariable(publicKey);
        if(includeLink)
            encoder.encodeVariable(link.toBytes());
        return encoder;
    }
    public static IdentityTriple decodeNextOrNull(MessageEncoder m) {
        String name = m.nextVariableString();
        if(name == null) return null;
        byte[] publicKey = m.nextVariable();
        P2Link link = P2Link.from(m.nextVariable());
        return new IdentityTriple(name, publicKey, link);
    }

    @Override public String toString() {
        return "IdentityTriple{" + "name='" + name + '\'' + ", publicKey=" + Arrays.toString(publicKey) + ", address=" + link + '}';
    }
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdentityTriple that = (IdentityTriple) o;
        return Objects.equals(name, that.name) && Arrays.equals(publicKey, that.publicKey) && Objects.equals(link, that.link);
    }
    @Override public int hashCode() {
        return 31 * Objects.hash(name, link) + Arrays.hashCode(publicKey);
    }

    public IdentityTriple linkWithRelayIfRequired(P2Link.Direct relay) {
        if(link.isOnlyLocal())
            return new IdentityTriple(name, publicKey, ((P2Link.Local) link).withRelay(relay));
        else
            return this;
    }
}