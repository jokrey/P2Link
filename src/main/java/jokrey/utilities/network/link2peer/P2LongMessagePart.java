package jokrey.utilities.network.link2peer;

import jokrey.utilities.network.link2peer.util.Hash;

import java.util.Arrays;

public class P2LongMessagePart extends P2LMessage {
    //todo merge index and size into a single field(first index always 0, size always the same from the first)
    //todo   additionally, a second flag is required (because the order in which packets arrieve is undefined)
    public final int index;
    public final int size;

    private P2LongMessagePart(String sender, int type, int conversationId, boolean requestReceipt, int index, int size, short expirationTimeoutInSeconds, Hash contentHash, byte[] raw, int payloadLength, byte[] payload) {
        super(sender, type, conversationId, requestReceipt, false, true, expirationTimeoutInSeconds, contentHash, raw, payloadLength, payload);
        this.index = index;
        this.size = size;
    }

    public static P2LongMessagePart from(P2LMessage longMessage) {
        if(!longMessage.isLongPart) throw new IllegalArgumentException();
        int index = HeaderUtil.readIndexFromLongHeader(longMessage.raw);
        int size = HeaderUtil.readSizeFromLongHeader(longMessage.raw);
        return new P2LongMessagePart(longMessage.sender, longMessage.type, longMessage.conversationId, longMessage.requestReceipt,
                index, size, longMessage.expirationTimeoutInSeconds, null, longMessage.raw, longMessage.payloadLength, null);
    }
    public static P2LongMessagePart from(P2LMessage message, int index, int size, int from, int to) {
        int subPayloadLength = to-from;
        byte[] raw = new byte[HeaderUtil.HEADER_SIZE_LONG_MESSAGE + subPayloadLength];
        HeaderUtil.writeHeader(raw, message.type, message.conversationId, message.requestReceipt, false, true, message.expirationTimeoutInSeconds);
        HeaderUtil.writeIndexToLongHeader(raw, index);
        HeaderUtil.writeSizeToLongHeader(raw, size);
        System.arraycopy(message.raw, from, raw, HeaderUtil.HEADER_SIZE_LONG_MESSAGE, subPayloadLength);
        return new P2LongMessagePart(message.sender, message.type, message.conversationId, message.requestReceipt, index, size, message.expirationTimeoutInSeconds, null, raw, subPayloadLength, null);
    }

    public static P2LMessage reassemble(P2LongMessagePart[] parts, int totalByteSize) {
        byte[] raw = new byte[(HeaderUtil.HEADER_SIZE_NORMAL_MESSAGE + totalByteSize)];
        int raw_i = P2LMessage.HeaderUtil.HEADER_SIZE_NORMAL_MESSAGE;
        for(P2LongMessagePart part:parts) {
            System.arraycopy(part.raw, P2LMessage.HeaderUtil.HEADER_SIZE_LONG_MESSAGE, raw, raw_i, part.payloadLength);
            raw_i+=part.payloadLength;
        }
        HeaderUtil.writeHeader(raw, parts[0].type, parts[0].conversationId, parts[0].requestReceipt, false, false, parts[0].expirationTimeoutInSeconds);
        return new P2LMessage(parts[0].sender, parts[0].type, parts[0].conversationId, parts[0].requestReceipt, false, false, parts[0].expirationTimeoutInSeconds, null, raw, totalByteSize, null);

    }
}