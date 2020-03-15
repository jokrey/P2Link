package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.message_headers.StreamReceiptHeader;
import jokrey.utilities.network.link2peer.util.LongTupleList;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

/**
 * @author jokrey
 */
class P2LFragmentStreamReceipt {
    final int receiptID;
    final long latestReceived;
    final boolean eof;
    final LongTupleList missingRanges;
    private P2LFragmentStreamReceipt(LongTupleList missingRanges, long latestReceived, boolean eof, int receiptID) {
        this.latestReceived = latestReceived;
        this.eof = eof;
        this.missingRanges = missingRanges;
        this.receiptID = receiptID;
    }

    static P2LMessage encode(int type, int conversationId, int step, boolean eof, long latestReceived, LongTupleList missingParts, int receiptID, int maxPackageSize) {
        StreamReceiptHeader header = new StreamReceiptHeader(null, toShort(type), toShort(conversationId), toShort(step), eof);
        int maxPayloadLength = missingParts.size()*2 *8 + 8 + 4;
        byte[] raw = header.generateRaw(maxPayloadLength, maxPackageSize); //this max package size enforces that only the first x fitting packages are packed, the rest is just discarded
        int raw_i = header.getSize();
        BitHelper.writeInt64(raw, raw_i, latestReceived);
        raw_i+=8;
        BitHelper.writeInt32(raw, raw_i, receiptID);
        raw_i+=4;
        for(int i=0;i<missingParts.size() && raw_i+16<=raw.length;i++) {
            long rE = missingParts.get1(i);
//            if(rE >= filterMissingAfter) continue; //NOW DONE BY THE RECEIVER...
            long rS = missingParts.get0(i);
            BitHelper.writeInt64(raw, raw_i, rS);
            raw_i+=8;
            BitHelper.writeInt64(raw, raw_i, rE);
            raw_i+=8;
        }
        return new P2LMessage(header, null, raw, raw_i-header.getSize());
    }
    static P2LFragmentStreamReceipt decode(P2LMessage message) {
        long latestReceived = message.nextLong();
        int receiptID = message.nextInt();
        int numberOfMissingParts = (message.getPayloadLength() - 8 - 4)/8;
        LongTupleList missingParts = new LongTupleList(numberOfMissingParts);
        for(int i=0;i<numberOfMissingParts/2;i++)
            missingParts.add(message.nextLong(), message.nextLong());
        return new P2LFragmentStreamReceipt(missingParts, latestReceived, message.header.isStreamEof(), receiptID);
    }
}
