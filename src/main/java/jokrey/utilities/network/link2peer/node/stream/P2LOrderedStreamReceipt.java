package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.message_headers.StreamReceiptHeader;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

/**
 * @author jokrey
 */
class P2LOrderedStreamReceipt {
    final int latestReceived;
    final boolean eof;
    final int[] missingParts;
    private P2LOrderedStreamReceipt(int latestReceived, boolean eof, int[] missingParts) {
        this.latestReceived = latestReceived;
        this.eof = eof;
        this.missingParts = missingParts;
    }

    static P2LMessage encode(int type, int conversationId, int step, boolean eof, int latestReceived, int... missingParts) {
        StreamReceiptHeader header = new StreamReceiptHeader(toShort(type), toShort(conversationId), toShort(step), eof);
        int payloadLength = missingParts.length*4 + 4;
        byte[] raw = header.generateRaw(payloadLength);
        int raw_i = header.getSize();
        BitHelper.writeInt32(raw, raw_i, latestReceived);
        raw_i+=4;
        for(int missingPart:missingParts) {
            BitHelper.writeInt32(raw, raw_i, missingPart);
            raw_i+=4;
        }
        return new P2LMessage(header, null, raw, payloadLength);
    }
    static P2LOrderedStreamReceipt decode(P2LMessage message) {
        int latestReceived = message.nextInt();
        int numberOfMissingParts = (message.getPayloadLength() - 4)/4;
        int[] missingParts = new int[numberOfMissingParts];
        for(int i=0;i<missingParts.length;i++)
            missingParts[i] = message.nextInt();
        return new P2LOrderedStreamReceipt(latestReceived, message.header.isStreamEof(), missingParts);
    }
}
