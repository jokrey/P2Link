package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.*;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.net.InetSocketAddress;
import java.util.*;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.*;

/**
 * NOTE:
 *   if someone expects a message - times out and THEN the message is received -> the message will remain in the maps  (solution timeouts for messages - given by client with max)
 *      CALLED EXPIRATION AND IS IMPLEMENTED
 *   if someone expects the same message before it times out, it will find the previous message, despite it not being the expected message (stack mildly mitigates this problem, but not a lot)
 *      also that new message will then remained in the maps for a while
 */
public class P2LMessageReceivalQueue {
    private final Map<HeaderIdentifier, Deque<P2LFuture<ReceivedP2LMessage>>> waitingReceivers = new HashMap<>();
    private final Map<HeaderIdentifier, List<ReceivedP2LMessage>> unconsumedMessages_byExactRequest = new HashMap<>();
    private final Map<Short, List<ReceivedP2LMessage>> unconsumedMessages_byId = new HashMap<>();
    synchronized void clear() {
        waitingReceivers.clear();
        unconsumedMessages_byExactRequest.clear();
        unconsumedMessages_byId.clear();
    }

    synchronized P2LFuture<ReceivedP2LMessage> futureFor(short messageType) {
        P2LFuture<ReceivedP2LMessage> future = new P2LFuture<>();
        List<ReceivedP2LMessage> unconsumedMessagesForId = unconsumedMessages_byId.get(messageType);
        if(unconsumedMessagesForId == null || unconsumedMessagesForId.isEmpty()) {
            waitingReceivers.computeIfAbsent(new TypeIdentifier(messageType), (k) -> new LinkedList<>()).push(future);
        } else {
            ReceivedP2LMessage consumedMessage = unconsumedMessagesForId.remove(0);
            unconsumedMessages_byExactRequest.get(new SenderTypeConversationIdentifier(consumedMessage)).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }
    synchronized P2LFuture<ReceivedP2LMessage> futureFor(InetSocketAddress from, short messageType) {
        if(from == null) return futureFor(messageType);
        return futureFor(from, messageType, NO_CONVERSATION_ID);
    }
    synchronized P2LFuture<ReceivedP2LMessage> futureFor(InetSocketAddress from, short messageType, short conversationId) {
        if(from == null) throw new NullPointerException("from cannot be null here");

        SenderTypeConversationIdentifier request = new SenderTypeConversationIdentifier(from, messageType, conversationId);

        P2LFuture<ReceivedP2LMessage> future = new P2LFuture<>();
        List<ReceivedP2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty()) {
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedList<>()).push(future);
        } else {
            ReceivedP2LMessage consumedMessage = unconsumedMessagesForRequest.remove(0);
            unconsumedMessages_byId.get(consumedMessage.header.getType()).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }

    public synchronized P2LFuture<ReceivedP2LMessage> futureFor(InetSocketAddress from, short messageType, short conversationId, short step) {
        if(from == null) return futureFor(messageType);
        System.out.println("futureFor - from = [" + from + "], messageType = [" + messageType + "], conversationId = [" + conversationId + "], step = [" + step + "]");
        return futureFor(new SenderTypeConversationIdStepIdentifier(from, messageType, conversationId, step));
    }
    synchronized P2LFuture<ReceivedP2LMessage> receiptFutureFor(InetSocketAddress from, short messageType, short conversationId) {
        if(from == null) return futureFor(messageType);
        return futureFor(new ReceiptIdentifier(from, messageType, conversationId));
    }
    public synchronized P2LFuture<ReceivedP2LMessage> receiptFutureFor(InetSocketAddress from, short messageType, short conversationId, short step) {
        if(from == null) return futureFor(messageType);
        System.out.println("receiptFutureFor - from = [" + from + "], messageType = [" + messageType + "], conversationId = [" + conversationId + "], step = [" + step + "]");
        return futureFor(new StepReceiptIdentifier(from, messageType, conversationId, step));
    }

    synchronized P2LFuture<ReceivedP2LMessage> futureFor(HeaderIdentifier request) {
        P2LFuture<ReceivedP2LMessage> future = new P2LFuture<>();
        List<ReceivedP2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty())
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedList<>()).push(future);
        return future;
    }

    public boolean handleNewMessage(ReceivedP2LMessage received) {
        //this seems dumb or overkill:
        HeaderIdentifier answersRequest =
                (received.header.isConversationPart()) ?
                        (
                                received.header.isReceipt() ?
                                        new StepReceiptIdentifier(received)
                                        :
                                        new SenderTypeConversationIdStepIdentifier(received)
                        ) : (
                        received.header.isReceipt() ?
                                new ReceiptIdentifier(received)
                                :
//                                received.header.isConversationIdPresent()?
                                    new SenderTypeConversationIdentifier(received)//: CANNOT GET MORE EXACT, BECAUSE OTHERWISE THE MATCHING WOULD FAIL
//                                    new SenderTypeIdentifier(received)
                );
        TypeIdentifier answersTypeRequest = received.header.getStep() != P2LMessageHeader.NO_STEP ? null : new TypeIdentifier(received);

        P2LFuture<ReceivedP2LMessage> toComplete;
        synchronized (this) {
            Deque<P2LFuture<ReceivedP2LMessage>> waitingForMessage = waitingReceivers.get(answersRequest); //higher priority
            Deque<P2LFuture<ReceivedP2LMessage>> waitingForMessageId = answersTypeRequest == null ? null : waitingReceivers.get(answersTypeRequest);

            while (true) {
                toComplete = null;
                if (waitingForMessage != null && !waitingForMessage.isEmpty()) {
                    toComplete = waitingForMessage.pop();
                    if (waitingForMessage.isEmpty())
                        waitingReceivers.remove(answersRequest, waitingForMessage);
                }
                if (toComplete == null && waitingForMessageId != null && !waitingForMessageId.isEmpty()) {
                    toComplete = waitingForMessageId.pop();
                    if (waitingForMessageId.isEmpty())
                        waitingReceivers.remove(answersTypeRequest, waitingForMessageId);
                }

                if (toComplete == null) {
                    if (!received.header.isExpired() && answersTypeRequest != null) {
                        unconsumedMessages_byId.computeIfAbsent(answersTypeRequest.messageType, (k) -> new LinkedList<>()).add(received);
                        unconsumedMessages_byExactRequest.computeIfAbsent(answersRequest, (k) -> new LinkedList<>()).add(received);
                    }
                    return false;
                } else {
                    if (!toComplete.isLikelyInactive()) {
                        break; //end synchronized block and set completed.
                    }
                }
            }
        }

        toComplete.setCompleted(received);
        return true;
    }


    synchronized void clean() {
        cleanExpiredMessages(unconsumedMessages_byExactRequest);
        cleanExpiredMessages(unconsumedMessages_byId);

        Iterator<Map.Entry<HeaderIdentifier, Deque<P2LFuture<ReceivedP2LMessage>>>> unconsumedIterator = waitingReceivers.entrySet().iterator();
        while(unconsumedIterator.hasNext()) {
            Map.Entry<HeaderIdentifier, Deque<P2LFuture<ReceivedP2LMessage>>> next = unconsumedIterator.next();
            next.getValue().removeIf(fut -> fut.isCanceled() || (fut.hasTimedOut() && !fut.isWaiting()));
            if(next.getValue().isEmpty())
                unconsumedIterator.remove();
        }
    }

    private static <T>void cleanExpiredMessages(Map<T, List<ReceivedP2LMessage>> unconsumedMessages) {
        Iterator<Map.Entry<T, List<ReceivedP2LMessage>>> unconsumedIterator = unconsumedMessages.entrySet().iterator();
        while(unconsumedIterator.hasNext()) {
            Map.Entry<T, List<ReceivedP2LMessage>> next = unconsumedIterator.next();
            next.getValue().removeIf(P2LMessage::isExpired);
            if(next.getValue().isEmpty())
                unconsumedIterator.remove();
        }
    }



    public String debugString() {
        return "P2LMessageQueue{" +
                "waitingReceivers("+waitingReceivers.size()+")=" + waitingReceivers +
                ", unconsumedMessages_byExactRequest("+unconsumedMessages_byExactRequest.size()+")=" + unconsumedMessages_byExactRequest +
                ", unconsumedMessages_byId("+unconsumedMessages_byExactRequest.size()+")=" + unconsumedMessages_byId +
                '}';
    }
}
