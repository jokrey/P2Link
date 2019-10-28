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
 */
class P2LMessageQueue {
    private final Map<MessageRequest, Stack<P2LFuture<P2LMessage>>> waitingReceivers = new HashMap<>();
    private final Map<MessageRequest, List<P2LMessage>> unconsumedMessages_byExactRequest = new HashMap<>();
    private final Map<Integer, List<P2LMessage>> unconsumedMessages_byId = new HashMap<>();
    synchronized P2LFuture<P2LMessage> futureFor(int msgId) {
        //System.out.println("futureFor1 - msgId = [" + msgId + "]");
        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForId = unconsumedMessages_byId.get(msgId);
        if(unconsumedMessagesForId == null || unconsumedMessagesForId.isEmpty()) {
            waitingReceivers.computeIfAbsent(new MessageRequest(msgId), (k) -> new LinkedStack<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForId.remove(0);
            unconsumedMessages_byExactRequest.get(new MessageRequest(consumedMessage)).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        //System.out.println("futureFor1 - waitingReceivers = " + waitingReceivers);
        //System.out.println("futureFor1 - unconsumedMessages_byId = " + unconsumedMessages_byId);
        //System.out.println("futureFor1 - unconsumedMessages_byExactRequest = " + unconsumedMessages_byExactRequest);
        return future;
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, int msgId) {
        return futureFor(WhoAmIProtocol.toString(from), msgId);
    }
    synchronized P2LFuture<P2LMessage> futureFor(String from, int msgId) {
        if(from == null) return futureFor(msgId);
        return futureFor(from, msgId, P2LNode.NO_CONVERSATION_ID);
    }
    synchronized P2LFuture<P2LMessage> futureFor(SocketAddress from, int msgId, int convId) {
        return futureFor(WhoAmIProtocol.toString(from), msgId, convId);
    }
    synchronized P2LFuture<P2LMessage> futureFor(String from, int msgId, int convId) {
        if(from == null) throw new NullPointerException("from cannot be null here");
        //System.out.println("futureFor2 - "+"from = [" + from + "], "+"msgId = [" + msgId + "], "+"convId = [" + convId + "]");

        MessageRequest request = new MessageRequest(from, msgId, convId, false);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty()) {
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedStack<>()).push(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForRequest.remove(0);
            unconsumedMessages_byId.get(consumedMessage.type).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
        //System.out.println("futureFor2 - waitingReceivers = " + waitingReceivers);
        //System.out.println("futureFor2 - unconsumedMessages_byId = " + unconsumedMessages_byId);
        //System.out.println("futureFor2 - unconsumedMessages_byExactRequest = " + unconsumedMessages_byExactRequest);
        return future;
    }

    synchronized P2LFuture<P2LMessage> receiptFutureFor(SocketAddress from, int msgId, int convId) {
        if(from == null) return futureFor(msgId);
        MessageRequest request = new MessageRequest(WhoAmIProtocol.toString(from), msgId, convId, true);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byExactRequest.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty())
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedStack<>()).push(future);
        return future;
    }
    synchronized void handleNewMessage(P2LMessage received) {
        //System.out.println("P2LMessageQueue.handleNewMessage - received = [" + received + "]");
        MessageRequest answersRequest = new MessageRequest(received);

        Stack<P2LFuture<P2LMessage>> waitingForMessage = waitingReceivers.get(answersRequest); //higher priority
        Stack<P2LFuture<P2LMessage>> waitingForMessageId = waitingReceivers.get(new MessageRequest(received.type));

        P2LFuture<P2LMessage> toComplete;
        while (true) {
            toComplete = waitingForMessage==null?null:waitingForMessage.pop();
            //System.out.println("1 toComplete = " + toComplete);
            if (toComplete == null)
                toComplete = waitingForMessageId==null?null:waitingForMessageId.pop();
            //System.out.println("2 toComplete = " + toComplete);

            if (toComplete == null) {
                if(! received.isExpired()) {
                    unconsumedMessages_byId.computeIfAbsent(answersRequest.msgId, (k) -> new LinkedList<>()).add(received);
                    unconsumedMessages_byExactRequest.computeIfAbsent(answersRequest, (k) -> new LinkedList<>()).add(received);
                }
                break;
            } else {
                //System.out.println("3 toComplete.isWaiting = " + toComplete.isWaiting());
                if(!toComplete.isCanceled() && (!toComplete.hasTimedOut() || toComplete.isWaiting())) {
                    toComplete.setCompleted(received);
                    break;
                }
            }
        }

        //System.out.println("handleNewMessage - waitingReceivers = " + waitingReceivers);
        //System.out.println("handleNewMessage - unconsumedMessages_byId = " + unconsumedMessages_byId);
        //System.out.println("handleNewMessage - unconsumedMessages_byExactRequest = " + unconsumedMessages_byExactRequest);
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


    private class MessageRequest {
        private final String from;
        private final int msgId;
        private final int convId;
        private final boolean msgIsReceipt;

        public MessageRequest(int msgId) {
            this(null, msgId, P2LNode.NO_CONVERSATION_ID, false);
        }
        public MessageRequest(String from, int msgId, int convId, boolean msgIsReceipt) {
            this.from = from;
            this.msgId = msgId;
            this.convId = convId;
            this.msgIsReceipt = msgIsReceipt;
        }
        public MessageRequest(P2LMessage msg) {
            this(msg.sender, msg.type, msg.conversationId, msg.isReceipt);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageRequest that = (MessageRequest) o;
            return msgId == that.msgId && Objects.equals(from, that.from) && msgIsReceipt == that.msgIsReceipt && convId == that.convId;
        }
        @Override public int hashCode() { return Objects.hash(from, msgId, convId, msgIsReceipt); }
        @Override public String toString() {
            return "MessageRequest{from=" + from + ", msgId=" + msgId + ", convId=" + convId + ", msgIsReceipt=" + msgIsReceipt + '}';
        }
    }
}
