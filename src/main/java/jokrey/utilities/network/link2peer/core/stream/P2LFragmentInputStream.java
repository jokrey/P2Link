package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LHeuristics;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.DataChunk;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;

/**
 * TODO this entire function is just a basic concept at this point
 *
 * Fragments other than stream parts do not have a package index, but rather a 'start' byte offset value.
 *    Unlike Streams this requires a mapping of said start index to a buffer, because it can no longer be directly calculated from the index and earliest received index.
 *
 * Ranges (problem on dynamic package size, which is required because mtu might change and generally required by udp and stream logic)
 *
 *
 * Every k(for example:1000) ms a receipt is automatically send, if no receipt was triggered in the last k ms (by being m(for example 10_000_000 bytes ahead))
 *
 *
 * Idea: System for fast file transfer in regularly reliable system (i.e. systems that can be expected to have a large bandwidth)
 *
 * @author jokrey
 */
public class P2LFragmentInputStream {
    protected final P2LNodeInternal parent;
    protected final SocketAddress to;
    protected final int type, conversationId;
    protected P2LFragmentInputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        this.parent = parent;
        this.to = to;
        this.type = type;
        this.conversationId = conversationId;
    }


    private HashMap<Integer, Integer> mappingOfStartToEndIndexOfMissingRanges = new HashMap<>(P2LHeuristics.STREAM_CHUNK_BUFFER_ARRAY_SIZE);
    private int latestReceivedEndIndex = 0;

    void received(P2LMessage message) throws IOException {
        int receivedRangeStartIndex = message.header.getPartIndex();
        int receivedRangeEndIndex = receivedRangeStartIndex + message.payloadLength;
        Integer missingRangeEndIndex = mappingOfStartToEndIndexOfMissingRanges.remove(receivedRangeStartIndex);
        if(missingRangeEndIndex != null) {
            if(receivedRangeEndIndex < missingRangeEndIndex) {
                mappingOfStartToEndIndexOfMissingRanges.put(receivedRangeEndIndex, missingRangeEndIndex); //new missing range inserted
            } else if(receivedRangeEndIndex > missingRangeEndIndex)
                throw new IllegalStateException("bug");
        } else { //newest package received
            //todo - could also be in between some unreceived range - requires regularly search

            latestReceivedEndIndex = receivedRangeEndIndex;
        }

        DataChunk received = new DataChunk(message);
        fragmentReceived(receivedRangeStartIndex, received);

        if(message.header.requestReceipt()) {
            parent.sendInternalMessage(StreamReceipt.encode(type, conversationId, false, latestReceivedEndIndex, getMissingRanges()), to);
        }
    }

    private int[] getMissingRanges() {
        return new int[0];
    }


    void fragmentReceived(int startIndex, DataChunk data) {
        //can be cached until order is available - in which case a maximum is required before new packages are dropped ( which has to be respected by the sender for efficiency reasons )

        //can be directly written to disk - on file transfer the file can be written to 'randomly', i.e. later parts written first(using randomaccessfile) - earlier parts are automatically re-requested
    }
}
