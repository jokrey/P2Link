package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2LNode;
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
    private final Map<MessageRequest, Stack<P2LFuture<P2LMessage>>> waitingReceivers = new HashMap<>();
    private final Map<MessageRequest, List<P2LMessage>> unconsumedMessages_byExactRequest = new HashMap<>();
    private final Map<Integer, List<P2LMessage>> unconsumedMessages_byId = new HashMap<>();
    synchronized P2LFuture<P2LMessage> futureFor(int messageType) {
        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForId = unconsumedMessages_byId.get(messageType);
        if(unconsumedMessagesForId == null || unconsumedMessagesForId.isEmpty()) {
            waitingReceivers.computeIfAbsent(new MessageRequest(messageType), (k) -> new LinkedStack<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForId.remove(0);
            unconsumedMessages_byExactRequest.get(new MessageRequest(consumedMessage)).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, int messageType) {
        return futureFor(WhoAmIProtocol.toString(from), messageType);
    }
    synchronized P2LFuture<P2LMessage> futureFor(String from, int messageType) {
        if(from == null) return futureFor(messageType);
        return futureFor(from, messageType, P2LNode.NO_CONVERSATION_ID);
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, int messageType, int conversationId) {
        return futureFor(WhoAmIProtocol.toString(from), messageType, conversationId);
    }
    private synchronized P2LFuture<P2LMessage> futureFor(String from, int messageType, int conversationId) {
        if(from == null) throw new NullPointerException("from cannot be null here");

        MessageRequest request = new MessageRequest(from, messageType, conversationId, false);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty()) {
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedStack<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForRequest.remove(0);
            unconsumedMessages_byId.get(consumedMessage.type).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        return future;
    }

    synchronized P2LFuture<P2LMessage> receiptFutureFor(SocketAddress from, int messageType, int conversationId) {
        if(from == null) return futureFor(messageType);
        MessageRequest request = new MessageRequest(WhoAmIProtocol.toString(from), messageType, conversationId, true);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty())
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedStack<>()).push(future);
        return future;
    }
    synchronized void handleNewMessage(P2LMessage received) {
        MessageRequest answersRequest = new MessageRequest(received);

        Stack<P2LFuture<P2LMessage>> waitingForMessage = waitingReceivers.get(answersRequest); //higher priority
        Stack<P2LFuture<P2LMessage>> waitingForMessageId = waitingReceivers.get(new MessageRequest(received.type));

        P2LFuture<P2LMessage> toComplete;
        while (true) {
            toComplete = waitingForMessage==null?null:waitingForMessage.pop();
            if (toComplete == null)
                toComplete = waitingForMessageId==null?null:waitingForMessageId.pop();

            if (toComplete == null) {
                if(! received.isExpired()) {
                    unconsumedMessages_byId.computeIfAbsent(answersRequest.messageType, (k) -> new LinkedList<>()).add(received);
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
        Iterator<Map.Entry<T, List<P2LMessage>>> byExact = unconsumedMessages.entrySet().iterator();
        while(byExact.hasNext()) {
            Map.Entry<T, List<P2LMessage>> next = byExact.next();
            next.getValue().removeIf(P2LMessage::isExpired);
            if(next.getValue().isEmpty())
                byExact.remove();
        }
    }


    private static class MessageRequest {
        private final String from;
        private final int messageType;
        private final int conversationId;
        private final boolean msgIsReceipt;
        private MessageRequest(int messageType) {
            this(null, messageType, P2LNode.NO_CONVERSATION_ID, false);
        }
        private MessageRequest(String from, int messageType, int conversationId, boolean msgIsReceipt) {
            this.from = from;
            this.messageType = messageType;
            this.conversationId = conversationId;
            this.msgIsReceipt = msgIsReceipt;
        }
        private MessageRequest(P2LMessage msg) {
            this(msg.sender, msg.type, msg.conversationId, msg.isReceipt);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageRequest that = (MessageRequest) o;
            return messageType == that.messageType && Objects.equals(from, that.from) && msgIsReceipt == that.msgIsReceipt && conversationId == that.conversationId;
        }
        @Override public int hashCode() { return Objects.hash(from, messageType, conversationId, msgIsReceipt); }
        @Override public String toString() {
            return "MessageRequest{from=" + from + ", messageType=" + messageType + ", conversationId=" + conversationId + ", msgIsReceipt=" + msgIsReceipt + '}';
        }
    }
}
