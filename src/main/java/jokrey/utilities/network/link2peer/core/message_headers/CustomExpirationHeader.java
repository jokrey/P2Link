package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class CustomExpirationHeader extends MinimalHeader {
    private final short expiresAfter;
    private final long createdAtCtm;
    public CustomExpirationHeader(String sender, int type, short expiresAfter, boolean requestReceipt) {
        super(sender, type, requestReceipt);
        this.expiresAfter = expiresAfter;
        createdAtCtm = System.currentTimeMillis();
    }

    @Override public short getExpiresAfter() {
        return expiresAfter;
    }
    @Override public boolean isExpired() {
        return createdAtCtm>0 && (expiresAfter <= 0 || (System.currentTimeMillis() - createdAtCtm)/1e3 > expiresAfter);
    }
}
