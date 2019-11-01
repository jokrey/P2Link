package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LMessageHeader;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jokrey
 */
public class LongMessageHandler {
    //todo requires some sort of short timeout - if the entire message is never received...
    private final ConcurrentHashMap<MessageIdentifier, MessagePartReceiver> map = new ConcurrentHashMap<>();

    public P2LMessage received(P2LMessage part) {
        MessageIdentifier identifier = new MessageIdentifier(part);
        MessagePartReceiver messages = map.computeIfAbsent(identifier, k -> new MessagePartReceiver(part.header.partNumberOfParts));
        messages.received(part);
        if(messages.isFullyReceived()) {
            map.remove(identifier);
            return messages.assemble();
        }
        return null;
    }

    public void send(P2LNodeInternal parent, P2LMessage overLongMessage, SocketAddress to) throws IOException {
        if(overLongMessage.canBeSentInSinglePacket()) throw new IllegalArgumentException("message could be send in a single packet...");
        int maxPayloadSize = P2LMessage.CUSTOM_RAW_SIZE_LIMIT -
                P2LMessageHeader.getSize(overLongMessage.header.isConversationIdPresent(), overLongMessage.header.isExpirationPresent(), true);
        //todo - this is a little dumb: the custom raw size limit is only to avoid involuntary fragmentation in layer 1+2 and keep a small buffer size when receiving
        //todo     - but now we are doing the fragmentation.. (only in java so it is much slower than in HW)
        //todo     - however if the receive buffer should generally remain small, I do not see an option
        int numberOfRequiredParts = overLongMessage.payloadLength / maxPayloadSize + 1;
        int lastPartSize = overLongMessage.payloadLength % maxPayloadSize;
        if(lastPartSize == 0) {
            numberOfRequiredParts--;
            lastPartSize = maxPayloadSize;
        }

        int from_raw_i = overLongMessage.header.getSize();
        for(int i=0;i<numberOfRequiredParts;i++) {
            int to_raw_i = from_raw_i + ((i+1==numberOfRequiredParts)? lastPartSize :maxPayloadSize);
            P2LMessage part = P2LMessage.Factory.messagePartFrom(overLongMessage, i, numberOfRequiredParts, from_raw_i, to_raw_i);
            parent.sendInternalMessage(part, to);
            from_raw_i = to_raw_i;
        }



        //TODO: UDT like stream protocol for VERY large messages (where package loss becomes likely and should not result in EVERYTHING being resend....)

        //todo - missing entire receipt and retry functionality....
        // todo - currently only valuable for mid sized messages (i.e. where it is likely that all parts arrive and can be retried in one using the conventional methods)
        // todo - would require an additional 'streaming functionality.

        //todo - every roughly second or so, a receipt of received messages is expected - otherwise the sending is halted
        //todo -     not received messages are resend - WITH NEW MESSAGES (up to a limit upon which the sending of new messages is halted
        //todo - if no receipt is received for twice roughly second or so - the sending of new messages is halted
    }


    //todo- receiver that writes to disk
    private static class MessagePartReceiver {
        private int numberOfPartsReceived = 0;
        private long totalByteSize = 0;
        private final P2LMessage[] parts;
        private MessagePartReceiver(int size) {
            parts = new P2LMessage[size];
        }
        synchronized void received(P2LMessage part) {
            if(parts[part.header.partIndex]==null) {
                parts[part.header.partIndex] = part;
                totalByteSize += part.payloadLength;
                numberOfPartsReceived++;
            }
            //else it is a resend message - ignore that it can happen, it is not that bad.. todo should not happen here... (unless conversation id is reused)
        }
        public boolean isFullyReceived() {
            return numberOfPartsReceived == parts.length;
        }

        //equals and hash code by pointer address
        @Override public String toString() {
            return "MessagePartReceiver{numberOfPartsReceived=" + numberOfPartsReceived + ", totalByteSize=" + totalByteSize + ", parts=" + Arrays.toString(parts) + '}';
        }

        public P2LMessage assemble() {
            if(!isFullyReceived()) throw new IllegalStateException();
            return P2LMessage.Factory.reassembleFromParts(parts, (int) totalByteSize);
        }
    }

    private static class MessageIdentifier {
        private final String from;
        private final int messageType;
        private final int conversationId;
        private MessageIdentifier(String from, int messageType, int conversationId) {
            this.from = from;
            this.messageType = messageType;
            this.conversationId = conversationId;
        }
        private MessageIdentifier(P2LMessage msg) {
            this(msg.header.sender, msg.header.type, msg.header.conversationId);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageIdentifier that = (MessageIdentifier) o;
            return messageType == that.messageType && Objects.equals(from, that.from) && conversationId == that.conversationId;
        }
        @Override public int hashCode() { return Objects.hash(from, messageType, conversationId); }
        @Override public String toString() {
            return "MessageRequest{from=" + from + ", messageType=" + messageType + ", conversationId=" + conversationId + '}';
        }
    }
}
