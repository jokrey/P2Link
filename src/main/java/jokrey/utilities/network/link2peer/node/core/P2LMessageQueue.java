package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.*;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.net.SocketAddress;
import java.util.*;

/**
 * TODO PROBLEM:
 *   if someone expects a message - times out and THEN the message is received -> the message will remain in the maps  (solution timeouts for messages - given by client with max)
 *   if someone expects the same message later, it will find this message, despite it not being the expected message (stack mildly mitigates this problem, but not a lot)
 */
public class P2LMessageQueue {
    private final Map<HeaderIdentifier, Deque<P2LFuture<P2LMessage>>> waitingReceivers = new HashMap<>();
    private final Map<HeaderIdentifier, List<P2LMessage>> unconsumedMessages_byExactRequest = new HashMap<>();
    private final Map<Short, List<P2LMessage>> unconsumedMessages_byId = new HashMap<>();
    synchronized void clear() {
        waitingReceivers.clear();
        unconsumedMessages_byExactRequest.clear();
        unconsumedMessages_byId.clear();
    }

    synchronized P2LFuture<P2LMessage> futureFor(short messageType) {
        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForId = unconsumedMessages_byId.get(messageType);
        if(unconsumedMessagesForId == null || unconsumedMessagesForId.isEmpty()) {
            waitingReceivers.computeIfAbsent(new TypeIdentifier(messageType), (k) -> new LinkedList<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForId.remove(0);
            unconsumedMessages_byExactRequest.get(new SenderTypeConversationIdentifier(consumedMessage)).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, short messageType) {
        if(from == null) return futureFor(messageType);
        return futureFor(from, messageType, P2LNode.NO_CONVERSATION_ID);
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, short messageType, short conversationId) {
        if(from == null) throw new NullPointerException("from cannot be null here");

        SenderTypeConversationIdentifier request = new SenderTypeConversationIdentifier(from, messageType, conversationId);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty()) {
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedList<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForRequest.remove(0);
            unconsumedMessages_byId.get(consumedMessage.header.getType()).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }

    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, short messageType, short conversationId, short step) {
        if(from == null) return futureFor(messageType);
        return futureFor(new SenderTypeConversationIdStepIdentifier(from, messageType, conversationId, step));
    }
    synchronized P2LFuture<P2LMessage> receiptFutureFor(SocketAddress from, short messageType, short conversationId) {
        if(from == null) return futureFor(messageType);
        return futureFor(new ReceiptIdentifier(from, messageType, conversationId));
    }
    synchronized P2LFuture<P2LMessage> receiptFutureFor(SocketAddress from, short messageType, short conversationId, short step) {
        if(from == null) return futureFor(messageType);
        return futureFor(new StepReceiptIdentifier(from, messageType, conversationId, step));
    }

    synchronized P2LFuture<P2LMessage> futureFor(HeaderIdentifier request) {
        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty())
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedList<>()).push(future);
        return future;
    }

    public synchronized boolean handleNewMessage(P2LMessage received) {
        //todo - this seems dumb:
        HeaderIdentifier answersRequest =
                (received.header.getStep() != P2LMessageHeader.NO_STEP) ?
                    (
                    received.header.isReceipt() ?
                        new StepReceiptIdentifier(received)
                        :
                        new SenderTypeConversationIdStepIdentifier(received)
                    ) : (
                    received.header.isReceipt() ?
                        new ReceiptIdentifier(received)
                        :
                        new SenderTypeConversationIdentifier(received)
                    );
        TypeIdentifier answersTypeRequest = received.header.getStep()!=P2LMessageHeader.NO_STEP?null:new TypeIdentifier(received);

//        if(received.header.isReceipt())
//            System.out.println("handleNewMessage[receipt] - answersRequest = " + answersRequest);

        Deque<P2LFuture<P2LMessage>> waitingForMessage = waitingReceivers.get(answersRequest); //higher priority
        Deque<P2LFuture<P2LMessage>> waitingForMessageId = answersTypeRequest==null?null:waitingReceivers.get(answersTypeRequest);

        P2LFuture<P2LMessage> toComplete;
        while (true) {
            toComplete = null;
            if(waitingForMessage!=null && !waitingForMessage.isEmpty()) {
                toComplete = waitingForMessage.pop();
                if(waitingForMessage.isEmpty())
                    waitingReceivers.remove(answersRequest, waitingForMessage);
            }
            if (toComplete == null && waitingForMessageId != null && !waitingForMessageId.isEmpty()) {
                toComplete = waitingForMessageId.pop();
                if(waitingForMessageId.isEmpty())
                    waitingReceivers.remove(answersTypeRequest, waitingForMessageId);
            }

            if (toComplete == null) {
                if(! received.header.isExpired() && answersTypeRequest != null) {
                    unconsumedMessages_byId.computeIfAbsent(answersTypeRequest.messageType, (k) -> new LinkedList<>()).add(received);
                    unconsumedMessages_byExactRequest.computeIfAbsent(answersRequest, (k) -> new LinkedList<>()).add(received);
                }
                return false;
            } else {
                if(!toComplete.isCanceled() && (!toComplete.hasTimedOut() || toComplete.isWaiting())) {
                    toComplete.setCompleted(received);
                    return true;
                }
            }
        }
    }


    synchronized void clean() {
        cleanExpiredMessages(unconsumedMessages_byExactRequest);
        cleanExpiredMessages(unconsumedMessages_byId);

        Iterator<Map.Entry<HeaderIdentifier, Deque<P2LFuture<P2LMessage>>>> unconsumedIterator = waitingReceivers.entrySet().iterator();
        while(unconsumedIterator.hasNext()) {
            Map.Entry<HeaderIdentifier, Deque<P2LFuture<P2LMessage>>> next = unconsumedIterator.next();
            next.getValue().removeIf(fut -> fut.isCanceled() || (fut.hasTimedOut() && !fut.isWaiting()));
            if(next.getValue().isEmpty())
                unconsumedIterator.remove();
        }
    }

    private static <T>void cleanExpiredMessages(Map<T, List<P2LMessage>> unconsumedMessages) {
        Iterator<Map.Entry<T, List<P2LMessage>>> unconsumedIterator = unconsumedMessages.entrySet().iterator();
        while(unconsumedIterator.hasNext()) {
            Map.Entry<T, List<P2LMessage>> next = unconsumedIterator.next();
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
