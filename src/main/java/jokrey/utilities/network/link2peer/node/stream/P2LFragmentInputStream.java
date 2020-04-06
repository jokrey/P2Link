package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.transparent_storage.bytes.TransparentBytesStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO this entire functionality is just a basic concept at this point
 *
 * Fragments other than stream parts do not have a package index, but rather a 'start' byte offset value.
 *    Unlike Streams this requires a mapping of said start index to a buffer, because it can no longer be directly calculated from the index and earliest received index.
 *
 * Ranges (problem on dynamic package size, which is required because mtu might change and generally required by udp and stream logic)
 *
 *
 * Every k ms(for example:1000ms) a receipt is automatically send, if no receipt was triggered in the last k ms (by being m(for example 10_000_000 bytes ahead))
 *
 *
 * Idea: System for fast file transfer in regularly reliable system (i.e. systems that can be expected to have a large bandwidth)
 *    however slow down should be valid
 *
 *
 * Concept:
 *   fragment: 'fragment' here shall denote a sequence of bytes in the context of more bytes, therefore a fragment has an associated offset and a length
 *   out
 *     1(init)- specify a 'data source(TransparentStorage)' from which fragments can be queried(by offset and length), the returned is a DataChunk (i.e. the underlying array can be the complete array)
 *        ((1.5- handshake(length - then the input stream could be fully responsible for re-querying packages, because it knows that data can be expected (delayed packages are less likely than dropped ones))
 *        without length: requery from output stream side by resending the earliest unconfirmed fragment
 *     2- the current data source can be send upon command (only fully - subsource command usable to send parts)
 *        2.5- ensures data is received (blocking here would have the advantage that no extra thread is required to requery packages - but that should be done by the other side anyways..)
 *
 * Congestion logic:
 *   parameters (determined automatically)
 *     average round time
 *     num packages lost with buf size n1, n2, etc. (up to max of 64)
 *       the complement is actually used: num packages received
 *       requires some sort of metric to determine what is best
 *           (for example - lost:0, size:1, recv:1  - is much worse than: lost:1, size:32, recv:31
 *              but       - lost:12, size:32, recv:20 - maybe worse than: lost:1, size:16, recv:15) - DUE TO RESEND (difference fragment stream - chunk stream)
 *           )
 *     (todo later: mtu - max limited by 8192, the custom raw size limit[tests have shown more is not better performance here])
 *
 * @author jokrey
 */
public abstract class P2LFragmentInputStream implements P2LInputStream {
    protected final P2LNodeInternal parent;
    protected final InetSocketAddress from;
    protected final P2LConnection con;
    protected final short type, conversationId, step;
    protected P2LFragmentInputStream(P2LNodeInternal parent, InetSocketAddress from, P2LConnection con, short type, short conversationId, short step) {
        this.parent = parent;
        this.from = from;
        this.con = con;
        this.type = type;
        this.conversationId = conversationId;
        this.step = step;
    }

    @Override public InetSocketAddress getRawFrom() { return from; }
    @Override public short getType() { return type; }
    @Override public short getConversationId() { return conversationId; }
    @Override public short getStep() { return step; }

    @Override public void close() throws IOException {
        listeners.clear();
    }


    //    private HashMap<Integer, Integer> mappingOfStartToEndIndexOfMissingRanges = new HashMap<>(P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE);
//    private int latestReceivedEndIndex = 0;
//
//    void received(P2LMessage message) throws IOException {
//        int receivedRangeStartIndex = message.header.getPartIndex();
//        int receivedRangeEndIndex = receivedRangeStartIndex + message.payloadLength;
//        Integer missingRangeEndIndex = mappingOfStartToEndIndexOfMissingRanges.remove(receivedRangeStartIndex);
//        if(missingRangeEndIndex != null) {
//            if(receivedRangeEndIndex < missingRangeEndIndex) {
//                mappingOfStartToEndIndexOfMissingRanges.put(receivedRangeEndIndex, missingRangeEndIndex); //new missing range inserted
//            } else if(receivedRangeEndIndex > missingRangeEndIndex)
//                throw new IllegalStateException("bug");
//        } else { //newest package received
//            //todo - could also be in between some unreceived range - requires a search
//
//            latestReceivedEndIndex = receivedRangeEndIndex;
//        }
//
//        DataChunk received = new DataChunk(message);
//        fragmentReceived(receivedRangeStartIndex, received);
//
//        if(message.header.requestReceipt()) {
//            parent.sendInternalMessage(StreamReceipt.encode(type, conversationId, false, latestReceivedEndIndex, getMissingRanges()), to);
//        }
//    }
//
//    private int[] getMissingRanges() {
//        return new int[0];
//    }

    public abstract boolean isFullyReceived();
    public abstract boolean waitForFullyReceived(int timeout_ms);

    private final List<FragmentReceivedListener> listeners = new ArrayList<>();
    public void addFragmentReceivedListener(FragmentReceivedListener listener) {
        listeners.add(listener);
    }
    public void removeFragmentReceivedListener(FragmentReceivedListener listener) {
        listeners.remove(listener);
    }
    public void writeResultsTo(TransparentBytesStorage storage) {
        addFragmentReceivedListener((start, part, off, len, eof) -> {if(part!=null)storage.set(start, part, off, len);});
    }

    public P2LFuture<Boolean> createAllReceivedFuture() {
        P2LFuture<Boolean> future = new P2LFuture<>();
        future.callMeBack(result -> {
            if(result == null) { //i.e. canceled from method caller / new owner of object
                try {
                    close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        addFragmentReceivedListener((fragmentOffset, receivedRaw, dataOff, dataLen, eof) -> {
            if(isFullyReceived())
                future.setCompleted(true);
        });
        if(isFullyReceived()) {
            return new P2LFuture<>(true);
        }
        return future;
    }

    protected void fireReceived(long fragmentOffset, byte[] receivedRaw, int dataOff, int dataLen, boolean eof) {
        for(FragmentReceivedListener listener:listeners) listener.received(fragmentOffset, receivedRaw, dataOff, dataLen, eof);
    }
    public interface FragmentReceivedListener {
        /**
         * @param fragmentOffset
         * @param receivedRaw
         * @param dataOff
         * @param dataLen
         * @param eof
         */
        void received(long fragmentOffset, byte[] receivedRaw, int dataOff, int dataLen, boolean eof);//todo potentially replace the three received vars with the DataChunk type
        //can be directly written to disk - on file transfer the file can be written to 'randomly', i.e. later parts written first(using randomaccessfile) - earlier parts are automatically re-requested

        //todo if this proves reasonable and possible the fragment stream could be used to implement a P2LInputStream(mildly less efficient):
        //  can be cached until order is available - in which case a maximum is required before new packages are dropped ( which has to be respected by the sender for efficiency reasons )
    }
}
