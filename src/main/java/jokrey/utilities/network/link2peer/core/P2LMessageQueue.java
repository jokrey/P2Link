package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.util.*;

class P2LMessageQueue {
    private final Map<MessageRequest, Queue<P2LFuture<P2LMessage>>> waitingReceivers = new HashMap<>();
    private final Map<MessageRequest, List<P2LMessage>> unconsumedMessages_byLink = new HashMap<>();
    private final Map<Integer, List<P2LMessage>> unconsumedMessages_byId = new HashMap<>();
    synchronized P2LFuture<P2LMessage> futureFor(int msgId) {
//        System.out.println("P2LMessageQueue.futureFor");
//        System.out.println("futureFor - msgId = [" + msgId + "]");
        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForId = unconsumedMessages_byId.get(msgId);
        if(unconsumedMessagesForId == null || unconsumedMessagesForId.isEmpty()) {
            waitingReceivers.computeIfAbsent(new MessageRequest(null, msgId), (k) -> new LinkedList<>()).add(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForId.remove(0);
            unconsumedMessages_byLink.get(new MessageRequest(consumedMessage)).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
//        System.out.println("futureFor1 - waitingReceivers = " + waitingReceivers);
//        System.out.println("futureFor1 - unconsumedMessages_byId = " + unconsumedMessages_byId);
//        System.out.println("futureFor1 - unconsumedMessages_byLink = " + unconsumedMessages_byLink);
        return future;
    }
    synchronized P2LFuture<P2LMessage> futureFor(P2Link from, int msgId) {
        if(from == null) return futureFor(msgId);
//        System.out.println("P2LMessageQueue.futureFor");
//        System.out.println("futureFor - from = [" + from + "], msgId = [" + msgId + "]");

        MessageRequest request = new MessageRequest(from, msgId);

        P2LFuture<P2LMessage> future = new P2LFuture<>();
        List<P2LMessage> unconsumedMessagesForRequest = unconsumedMessages_byLink.get(request);
        if(unconsumedMessagesForRequest == null || unconsumedMessagesForRequest.isEmpty()) {
            waitingReceivers.computeIfAbsent(request, (k) -> new LinkedList<>()).add(future);
        } else {
            P2LMessage consumedMessage = unconsumedMessagesForRequest.remove(0);
            unconsumedMessages_byId.get(consumedMessage.type).remove(consumedMessage);
            future.setCompleted(consumedMessage);
        }
//        System.out.println("futureFor2 - waitingReceivers = " + waitingReceivers);
//        System.out.println("futureFor2 - unconsumedMessages_byId = " + unconsumedMessages_byId);
//        System.out.println("futureFor2 - unconsumedMessages_byLink = " + unconsumedMessages_byLink);
        return future;
    }
    synchronized void handleNewMessage(P2LMessage received) {
//        System.out.println("P2LMessageQueue.handleNewMessage");
//        System.out.println("handleNewMessage - received = [" + received + "]");
        MessageRequest answersRequest = new MessageRequest(received.sender, received.type);

        Queue<P2LFuture<P2LMessage>> waitingForMessage = waitingReceivers.get(answersRequest); //higher priority
        Queue<P2LFuture<P2LMessage>> waitingForMessageId = waitingReceivers.get(new MessageRequest(null, received.type));

        P2LFuture<P2LMessage> toComplete;
        while (true) {
            toComplete = waitingForMessage==null?null:waitingForMessage.poll();
            if (toComplete == null)
                toComplete = waitingForMessageId==null?null:waitingForMessageId.poll();

            if (toComplete == null) {
                unconsumedMessages_byId.computeIfAbsent(answersRequest.msgId, (k) -> new LinkedList<>()).add(received);
                unconsumedMessages_byLink.computeIfAbsent(answersRequest, (k) -> new LinkedList<>()).add(received);
                break;
            }

            if(toComplete.isWaiting()) {
                toComplete.setCompleted(received);
                break;
            }
        }

//        System.out.println("handleNewMessage - waitingReceivers = " + waitingReceivers);
//        System.out.println("handleNewMessage - unconsumedMessages_byId = " + unconsumedMessages_byId);
//        System.out.println("handleNewMessage - unconsumedMessages_byLink = " + unconsumedMessages_byLink);
    }


    private class MessageRequest {
        private final P2Link from;
        private final int msgId;

        public MessageRequest(P2Link from, int msgId) {
            this.from = from;
            this.msgId = msgId;
        }
        public MessageRequest(P2LMessage msg) {
            this(msg.sender, msg.type);
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageRequest that = (MessageRequest) o;
            return msgId == that.msgId && Objects.equals(from, that.from);
        }
        @Override public int hashCode() { return Objects.hash(from, msgId); }
        @Override public String toString() {
            return "MessageRequest{from=" + from + ", msgId=" + msgId + '}';
        }
    }
}
