package jokrey.utilities.network.link2peer.core.message_headers;

/**
 * @author jokrey
 */
public class StreamPartHeader extends ConversationHeader {
    private final int index;
    public StreamPartHeader(String sender, int type, int conversationId, int index, boolean eofIndicator) {
        super(sender, type, conversationId, eofIndicator);

        //TODO - FINAL STREAM PART == REQUEST RECEIPT IS A WEIRD INTERNAL HACK THAT WILL WORK BUT IS A BIT WEIRD SO KEEP THAT IN MIND PLEASE

        this.index = index;
    }

    @Override public int getPartIndex() { return index; }
    @Override public boolean isStreamPart() { return true; }
    @Override public boolean isStreamEof() { return requestReceipt(); }
}
