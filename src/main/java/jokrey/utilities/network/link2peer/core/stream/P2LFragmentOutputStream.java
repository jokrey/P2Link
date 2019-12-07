package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.core.P2LConnection;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;
import jokrey.utilities.transparent_storage.bytes.wrapper.SubBytesStorage;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * @author jokrey
 */
public abstract class P2LFragmentOutputStream implements P2LOutputStream {
    protected final P2LNodeInternal parent;
    protected final SocketAddress to;
    protected final P2LConnection con;
    protected final int type;
    protected final int conversationId;
    protected FragmentRetriever source;
    protected P2LFragmentOutputStream(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.con = con;
        this.type = type;
        this.conversationId = conversationId;
    }


    public void setSource(FragmentRetriever source) {
        this.source = source;
    }
    public void setSource(byte[] toSend) {
        setSource(getRetrieverFor(new ByteArrayStorage(toSend)));
    }
    public void setSource(TransparentBytesStorage storage) {
        setSource(getRetrieverFor(storage));
    }

    public abstract void send() throws InterruptedException;


    public static FragmentRetriever getRetrieverFor(TransparentBytesStorage storage) {
        return new FragmentRetriever() {
            @Override public SubBytesStorage sub(long start, long end) {
                return storage.subStorage(start, end);
            }
            @Override public long currentMaxEnd() {
                return storage.contentSize();
            }
            @Override public long totalNumBytes() {
                return storage.contentSize();
            }
            @Override public void adviceEarliestRequiredIndex(long index) { }
        };
    }
    interface FragmentRetriever {
        SubBytesStorage sub(long start, long end);
        long currentMaxEnd();
        long totalNumBytes(); //can be -1
        void adviceEarliestRequiredIndex(long index);
    }
}
