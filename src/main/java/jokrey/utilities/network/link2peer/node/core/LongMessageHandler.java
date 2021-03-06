package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.HeaderIdentifier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author jokrey
 */
class LongMessageHandler {
    //todo requires some sort of short timeout - if the entire message is never received...
    private final ConcurrentHashMap<HeaderIdentifier, MessagePartReceiver> receivedPartsMap = new ConcurrentHashMap<>();
    private ReentrantReadWriteLock cleanUpLock = new ReentrantReadWriteLock();

    ReceivedP2LMessage received(ReceivedP2LMessage part) {
        cleanUpLock.readLock().lock();
        try {
            HeaderIdentifier identifier = new P2LMessageHeader.SenderTypeConversationIdStepIdentifier(part);
            if(part.header.getNumberOfParts() > P2LHeuristics.LONG_MESSAGE_MAX_NUMBER_OF_PARTS) {
                System.err.println("received long message part with size("+part.header.getNumberOfParts()+") > max("+P2LHeuristics.LONG_MESSAGE_MAX_NUMBER_OF_PARTS+"). " +
                        "Consider using a stream instead.");
                return null;
            }
            MessagePartReceiver messages = receivedPartsMap.computeIfAbsent(identifier, k -> new MessagePartReceiver(part.header.getNumberOfParts()));
            messages.received(part);
            if (messages.isFullyReceived()) {
                receivedPartsMap.remove(identifier);
                return messages.assemble();
            }
            return null;
        } finally {
            cleanUpLock.readLock().unlock();
        }
    }
    void clean() {
        cleanUpLock.writeLock().lock();
        try {
            receivedPartsMap.values().removeIf(MessagePartReceiver::isExpired);
        } finally {
            cleanUpLock.writeLock().unlock();
        }
    }

    void send(P2LNodeInternal parent, P2LMessage overLongMessage, InetSocketAddress to) throws IOException {
        if(overLongMessage.canBeSentInSinglePacket()) throw new IllegalArgumentException("message could be send in a single packet...");
        int maxPayloadSize = P2LMessage.CUSTOM_RAW_SIZE_LIMIT -
                P2LMessageHeader.getSize(overLongMessage.header.isConversationIdPresent(), overLongMessage.header.isExpirationPresent(), overLongMessage.header.isStepPresent(), true, false, false);
        //todo - this is a little dumb: the custom raw size limit is only to avoid involuntary fragmentation in layer 1+2 and keep a small buffer size when receiving
        //todo     - but now we are doing the fragmentation.. (only in java so it is much slower than in HW)
        //todo     - however if the receive buffer should generally remain small, I do not see another option - however using streams is encouraged at this point - thought not enforced as it can be overkill for just a few packages
        int numberOfRequiredParts = overLongMessage.getPayloadLength() / maxPayloadSize + 1;
        int lastPartSize = overLongMessage.getPayloadLength() % maxPayloadSize;
        if(lastPartSize == 0) {
            numberOfRequiredParts--;
            lastPartSize = maxPayloadSize;
        }

        int from_raw_i = overLongMessage.header.getSize();
        for(int i=0;i<numberOfRequiredParts;i++) {
            int to_raw_i = from_raw_i + ((i+1==numberOfRequiredParts)? lastPartSize :maxPayloadSize);
            P2LMessage part = P2LMessage.messagePartFrom(overLongMessage, i, numberOfRequiredParts, from_raw_i, to_raw_i);
//            System.out.println("sending part = " + part);
            parent.sendInternalMessage(to, part);
            from_raw_i = to_raw_i;
        }
    }


    private static class MessagePartReceiver {
        private long lastMessageReceivedAtCtm = System.currentTimeMillis();
        private int numberOfPartsReceived = 0;
        private long totalByteSize = 0;
        private final ReceivedP2LMessage[] parts;
        private MessagePartReceiver(int size) {
            parts = new ReceivedP2LMessage[size];
        }
        synchronized void received(ReceivedP2LMessage part) {
            if(parts.length <= part.header.getPartIndex()) {
//                System.out.println("part = " + part);
                throw new ArrayIndexOutOfBoundsException(part.header.getPartIndex()+"/"+parts.length);
            }
            if(parts[part.header.getPartIndex()]==null) {
                parts[part.header.getPartIndex()] = part;
                totalByteSize += part.getPayloadLength();
                numberOfPartsReceived++;
                lastMessageReceivedAtCtm = System.currentTimeMillis();
            }
            //else it is a resend message - ignore that it can happen, it is not that bad.. ----- should not happen here... (unless conversation id is reused)
        }
        boolean isFullyReceived() {
            return numberOfPartsReceived == parts.length;
        }
        boolean isExpired() {
            return (System.currentTimeMillis() - lastMessageReceivedAtCtm) > P2LHeuristics.LONG_MESSAGE_RECEIVE_NO_PART_TIMEOUT_MS;
            //even if the timeout is only 10 seconds, a slow loris attack is still feasible - send a very fragmented message of only a few bytes each and send them slowly (the timeout would be reset..)
        }

        //equals and hash code by pointer address
        @Override public String toString() {
            return "MessagePartReceiver{numberOfPartsReceived=" + numberOfPartsReceived + ", totalByteSize=" + totalByteSize + ", parts=" + Arrays.toString(parts) + '}';
        }

        ReceivedP2LMessage assemble() {
            if(!isFullyReceived()) throw new IllegalStateException();
            return P2LMessage.reassembleFromParts(parts, (int) totalByteSize);
        }
    }

    String debugString() {
        return "LongMessageHandler{receivedPartsMap=" + receivedPartsMap + '}';
    }
}
