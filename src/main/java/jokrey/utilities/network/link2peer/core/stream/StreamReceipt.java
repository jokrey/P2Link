package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.message_headers.StreamReceiptHeader;
import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.util.Arrays;

/**
 * @author jokrey
 */
public class StreamReceipt {
    static P2LMessage encode(int type, int conversationId, boolean eof, int latestReceived, int... missingParts) {
        StreamReceiptHeader header = new StreamReceiptHeader(null, type, conversationId, eof);
        int payloadLength = missingParts.length*4 + 4 + 4;
        byte[] raw = header.generateRaw(payloadLength);
        int raw_i = header.getSize();
        BitHelper.writeInt32(raw, raw_i, latestReceived);
        raw_i+=4;
        BitHelper.writeInt32(raw, raw_i, missingParts.length); //todo - firstly missing parts length will always be AT MOST 2 bytes(conversion to unsigned would yield 2^16 missing parts max), with default heuristics it could be 1
        raw_i+=4;
        for(int missingPart:missingParts) {
            BitHelper.writeInt32(raw, raw_i, missingPart); //todo - firstly missing parts length will always be AT MOST 2 bytes
            raw_i+=4;
        }
//        System.out.println("encode - missingParts = " + Arrays.toString(missingParts));
//        System.out.println("encode - latestReceived = " + latestReceived);
        return new P2LMessage(header, null, raw, payloadLength, null);
    }
    static Pair<Integer, int[]> decode(P2LMessage message) {
        int latestReceived = message.nextInt();
        int numberOfMissingParts = message.nextInt();
        int[] missingParts = new int[numberOfMissingParts];
        for(int i=0;i<missingParts.length;i++)
            missingParts[i] = message.nextInt();
        return new Pair<>(latestReceived, missingParts);
    }
}
