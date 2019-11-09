package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.HeaderIdentifier;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.ReceiptIdentifier;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.TypeIdentifier;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.simple.data_structure.stack.LinkedStack;
import jokrey.utilities.simple.data_structure.stack.Stack;

import java.net.SocketAddress;
import java.util.*;

/**
 * TODO PROBLEM:
 *   if someone expects a message - times out and THEN the message is received -> the message will remain in the maps  (solution timeouts for messages - given by client with max)
 *   if someone expects the same message later, it will find this message, despite it not being the expected message (stack mildly mitigates this problem, but not a lot)
 *
 * TODO: clean up canceled/timed out receivers
 */
class P2LMessageQueue {
    private final Map<HeaderIdentifier, Stack<P2LFuture<P2LMessage>>> waitingReceivers = new HashMap<>();
    private final Map<HeaderIdentifier, List<P2LMessage>> unconsumedMessages_byExactRequest = new HashMap<>();
    private final Map<Integer, List<P2LMessage>> unconsumedMessages_byId = new HashMap<>();
    synchronized void clear() {
        waitingReceivers.clear();
        unconsumedMessages_byExactRequest.clear();
        unconsumedMessages_byId.clear();
    }

    synchronized P2LFuture<P2LMessage> futureFor(int messageType) {
        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForId = unconsumedMessages_byId.get(messageType);
        if(unconsumedMessagesForId == null || unconsumedMessagesForId.isEmpty()) {
            waitingReceivers.computeIfAbsent(new TypeIdentifier(messageType), (k) -> new LinkedStack<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForId.remove(0);
            unconsumedMessages_byExactRequest.get(new SenderTypeConversationIdentifier(consumedMessage)).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, int messageType) {
        if(from == null) return futureFor(messageType);
        return futureFor(from, messageType, P2LNode.NO_CONVERSATION_ID);
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, int messageType, int conversationId) {
        if(from == null) throw new NullPointerException("from cannot be null here");

        SenderTypeConversationIdentifier request = new SenderTypeConversationIdentifier(from, messageType, conversationId);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty()) {
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedStack<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForRequest.remove(0);
            unconsumedMessages_byId.get(consumedMessage.header.getType()).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }

    synchronized P2LFuture<P2LMessage> receiptFutureFor(SocketAddress from, int messageType, int conversationId) {
        if(from == null) return futureFor(messageType);
        ReceiptIdentifier request = new ReceiptIdentifier(from, messageType, conversationId);
        System.out.println("receiptFutureFor - from = " + from);
        System.out.println("receiptFutureFor - request = " + request);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty())
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedStack<>()).push(future);
        return future;
    }
    synchronized void handleNewMessage(P2LMessage received) {
        HeaderIdentifier answersRequest = received.header.isReceipt()?new ReceiptIdentifier(received):new SenderTypeConversationIdentifier(received);
        TypeIdentifier answersTypeRequest = new TypeIdentifier(received);

        System.out.println("handleNewMessage - answersRequest = " + answersRequest);
        System.out.println("handleNewMessage - received.header.getSender().getSocketAddress = " + received.header.getSender().getSocketAddress());

        Stack<P2LFuture<P2LMessage>> waitingForMessage = waitingReceivers.get(answersRequest); //higher priority
        Stack<P2LFuture<P2LMessage>> waitingForMessageId = waitingReceivers.get(answersTypeRequest);

        P2LFuture<P2LMessage> toComplete;
        while (true) {
            toComplete = null;
            if(waitingForMessage!=null) {
                toComplete = waitingForMessage.pop();
                if(waitingForMessage.isEmpty())
                    waitingReceivers.remove(answersRequest, waitingForMessage);
            }
            if (toComplete == null && waitingForMessageId != null) {
                toComplete = waitingForMessageId.pop();
                if(waitingForMessageId.isEmpty())
                    waitingReceivers.remove(answersTypeRequest, waitingForMessageId);
            }

            if (toComplete == null) {
                if(! received.header.isExpired()) {
                    unconsumedMessages_byId.computeIfAbsent(answersTypeRequest.messageType, (k) -> new LinkedList<>()).add(received);
                    unconsumedMessages_byExactRequest.computeIfAbsent(answersRequest, (k) -> new LinkedList<>()).add(received);
                }
                break;
            } else {
                if(!toComplete.isCanceled() && (!toComplete.hasTimedOut() || toComplete.isWaiting())) {
                    toComplete.setCompleted(received);
                    break;
                }
            }
        }
    }


    synchronized void cleanExpiredMessages() {
        cleanExpiredMessages(unconsumedMessages_byExactRequest);
        cleanExpiredMessages(unconsumedMessages_byId);
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
