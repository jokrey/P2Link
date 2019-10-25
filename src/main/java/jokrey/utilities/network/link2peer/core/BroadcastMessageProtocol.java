package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.OutgoingHandler.Task;
import jokrey.utilities.network.link2peer.util.Hash;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * @author jokrey
 */
class BroadcastMessageProtocol {
    static void send(P2LNodeInternal parent, P2Link peer, P2LMessage message) throws IOException {
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        parent.send(P2LMessage.createSendMessage(SC_BROADCAST, message.getHash().raw()), peerConnection);

        P2LMessage peerHashKnowledgeOfMessage_msg = parent.futureForInternal(peer, C_BROADCAST_MSG_KNOWLEDGE_RETURN).get(5000);
        boolean peerHashKnowledgeOfMessage = peerHashKnowledgeOfMessage_msg.nextBool();

        //System.out.println("sending brd to "+connection.peerLink + " - peer has knowledge of message received - "+peerHashKnowledgeOfMessage);

        if(!peerHashKnowledgeOfMessage) {
            parent.send(
                    P2LMessage.createSendMessageFrom(C_BROADCAST_MSG, message.type,
                            P2LMessage.makeVariableIndicatorFor(message.sender.getRepresentingByteArray().length), message.sender.getRepresentingByteArray(),
                            P2LMessage.makeVariableIndicatorFor(message.payloadLength), message.asBytes()),
                    peerConnection);

            //todo - receipt required???
            //System.out.println("sending brd to "+connection.peerLink + " - send sender + data");
        }
    }

    static P2LMessage receive(P2LNodeInternal parent, BroadcastState state, SocketAddress peerConnection, P2LMessage initialMessage) throws IOException {
//        System.out.println("receiving message at " + parent.getSelfLink());

        Hash brdMessageHash = new Hash(initialMessage.asBytes());
        boolean wasKnown = state.markAsKnown(brdMessageHash);

//        System.out.println("receiving message at " + parent.getSelfLink() + " - got hash");

        if(wasKnown) {
//        if(state.isKnown(hash)) {
//            System.out.println("receiving message at " + parent.getSelfLink() + " - known - sending msg knowledge return");
            parent.send(P2LMessage.createSendMessage(C_BROADCAST_MSG_KNOWLEDGE_RETURN, true), peerConnection);
//            System.out.println("receiving message at " + parent.getSelfLink() + " - known - send msg knowledge return");
            return null; //do not tell application about broadcast again
        } else {

            try {

    //            System.out.println("receiving message at " + parent.getSelfLink() + " - NOT known - sending msg knowledge return - hash: "+ Arrays.toString(initialMessage.asBytes()));
                parent.send(P2LMessage.createSendMessage(C_BROADCAST_MSG_KNOWLEDGE_RETURN, false), peerConnection);
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - NOT known - send msg knowledge return");

                P2LMessage message = parent.futureForInternal(initialMessage.sender, C_BROADCAST_MSG).get(2500);
                int brdMsgType = message.nextInt();
                P2Link sender = new P2Link(message.nextVariableString());
                byte[] data = message.nextVariable();
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - read data");
                P2LMessage receivedMessage = P2LMessage.createBrdMessage(sender, brdMsgType, data);

                relayBroadcast(receivedMessage, parent, initialMessage.sender);
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - relayed broadcast");

                return receivedMessage;
            } catch (Throwable t) {
                state.markAsUnknown(brdMessageHash);
                return null;
            }

        }
    }

    //outgoing thread pool
    public static void relayBroadcast(P2LMessage message, P2LNodeInternal node, P2Link messageReceivedFrom) {
        Set<P2Link> links = node.getActivePeerLinks();
        ArrayList<P2Link> linksExcept = new ArrayList<>(links.size()-1);//not guaranteed that orig message sender is a direct peer, so NOT -2
        for(P2Link l:links)
            if(!l.equals(message.sender) && !l.equals(messageReceivedFrom))
                linksExcept.add(l);

        Task[] tasks = new Task[linksExcept.size()];
        for (int i = 0; i < linksExcept.size(); i++) {
            P2Link l = linksExcept.get(i);
            tasks[i] = () -> {
                send(node, l, message);
                return true;
            };
        }
        node.executeAllOnSendThreadPool(tasks); //required, because send also waits for a response...
        //TODO: 'short' broadcasts that do not do the hash thing (in that case it is not required to wait for a response...)
    }


    public static class BroadcastState {
        private ConcurrentHashMap<Hash, Long> knownMessageHashes = new ConcurrentHashMap<>();
        public boolean isKnown(Hash hash) {
            return knownMessageHashes.containsKey(hash);
        }
        //return if it was previously known
        public boolean markAsKnown(Hash hash) {
            long currentTime = System.currentTimeMillis();
            Long oldVal = knownMessageHashes.put(hash, currentTime);
//            System.out.println("hash = " + hash + " - oldVal: "+oldVal);

            if(knownMessageHashes.size() > 500) { //threshold to keep the remove if loop from being run always, maybe 'outsource' the clean up into a background thread
                //older than two minutes - honestly should be much more than enough to finish propagating a message
                knownMessageHashes.entrySet().removeIf(entry -> (currentTime - entry.getValue()) / 1000.0 > 60 * 2);
            }

            return oldVal != null; //there was a previous mapping - i.e. the message was previously known
        }

        public void markAsUnknown(Hash hash) {
            knownMessageHashes.remove(hash);
        }
    }
}