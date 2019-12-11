package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;
import jokrey.utilities.transparent_storage.bytes.non_persistent.ByteArrayStorage;

import java.net.SocketAddress;
import java.util.Objects;

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
    @Override public SocketAddress getRawFrom() { return to; }
    @Override public int getType() { return type; }
    @Override public int getConversationId() { return conversationId; }


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
            @Override public byte[] sub(Fragment fragment) {
                return storage.sub(fragment.realStartIndex, fragment.realEndIndex);
            }
            @Override public Fragment sub(long start, long end) {
                return new Fragment(this, start, end);
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
        byte[] sub(Fragment fragment);
        Fragment sub(long start, long end);
        long currentMaxEnd();
        long totalNumBytes(); //can be -1
        void adviceEarliestRequiredIndex(long index);
    }


    
    public static class Fragment {
        public final FragmentRetriever retriever;
        public final long realStartIndex;
        public final long realEndIndex;

        public Fragment(FragmentRetriever retriever, long start, long end) {
            this.retriever = retriever;
            realStartIndex = start;
            realEndIndex = end;
        }

        public byte[] content() {
            return retriever.sub(this);
        }
        public boolean isEmpty() {
            return realStartIndex == Math.min(realEndIndex, retriever.currentMaxEnd());
        }

        @Override
        public String toString() {
            return "Fragment{" +
                    "retriever=" + retriever +
                    ", realStartIndex=" + realStartIndex +
                    ", realEndIndex=" + realEndIndex +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Fragment fragment = (Fragment) o;
            return realStartIndex == fragment.realStartIndex &&
                    realEndIndex == fragment.realEndIndex &&
                    Objects.equals(retriever, fragment.retriever);
        }

        @Override
        public int hashCode() {
            return Objects.hash(retriever, realStartIndex, realEndIndex);
        }
    }
}
