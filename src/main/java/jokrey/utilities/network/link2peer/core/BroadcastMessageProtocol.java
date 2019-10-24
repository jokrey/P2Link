package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.OutgoingHandler.Task;
import jokrey.utilities.network.link2peer.util.Hash;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * @author jokrey
 */
class BroadcastMessageProtocol {
    public static boolean send(PeerConnection connection, P2LNodeInternal node, int msgId, byte[] message) {
        try {
            send(connection, new P2LMessage(node.getSelfLink(), msgId, message));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static void send(PeerConnection connection, P2LMessage message) throws IOException {
        connection.sendSuperCause(SC_BROADCAST);

        //System.out.println("sending brd to "+connection.peerLink + " - sending hash");
        connection.send(C_BROADCAST_HASH, message.getHash().raw());
        //System.out.println("sending brd to "+connection.peerLink + " - send hash");

        P2LMessage peerHashKnowledgeOfMessage_msg = connection.futureRead(C_BROADCAST_MSG_KNOWLEDGE_RETURN).get(5000);
        boolean peerHashKnowledgeOfMessage = peerHashKnowledgeOfMessage_msg.asBool();

        //System.out.println("sending brd to "+connection.peerLink + " - peer has knowledge of message received - "+peerHashKnowledgeOfMessage);

        if(!peerHashKnowledgeOfMessage) {
            connection.send(C_BROADCAST_MSG, new LIbae().encode(P2LMessage.fromInt(message.type), message.sender.getRepresentingByteArray(), message.data).getEncoded());
            //System.out.println("sending brd to "+connection.peerLink + " - send sender + data");
        }
    }

    public static P2LMessage receive(BroadcastState state, P2LNodeInternal selfNode, PeerConnection connection) throws IOException {
        //SUPER CAUSE HAS BEEN PREVIOUSLY RECEIVED

        //System.out.println("receiving message at " + selfNode.getSelfLink());

        Hash hash = new Hash(connection.futureRead(C_BROADCAST_HASH).get(5000).data);
        boolean wasKnown = state.markAsKnown(hash);

        //System.out.println("receiving message at " + selfNode.getSelfLink() + " - got hash");

        if(wasKnown) {
//        if(state.isKnown(hash)) {
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - known - sending msg knowledge return");
            connection.send(C_BROADCAST_MSG_KNOWLEDGE_RETURN, P2LMessage.fromBool(true));
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - known - send msg knowledge return");
            return null; //do not tell application over broadcast again
        } else {
//            state.markAsKnown(hash);

            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - NOT known - sending msg knowledge return");
            connection.send(C_BROADCAST_MSG_KNOWLEDGE_RETURN, P2LMessage.fromBool(false));
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - NOT known - send msg knowledge return");

            Iterator<byte[]> libae = new LIbae(connection.futureRead(C_BROADCAST_MSG).get(5000).data).iterator();
            int id = P2LMessage.trans.detransform_int(libae.next());
            P2Link sender = new P2Link(P2LMessage.trans.detransform_string(libae.next()));
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - read sender");
            byte[] data = libae.next();
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - read data");
            P2LMessage receivedMessage = new P2LMessage(sender, id, data);

            relayBroadcast(receivedMessage, selfNode, connection.peerLink);
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - relayed broadcast");

            //todo mark as unknown on error...

            return receivedMessage;
        }
    }

    //outgoing thread pool
    public static void relayBroadcast(P2LMessage message, P2LNodeInternal node, P2Link messageReceivedFrom) {
        PeerConnection[] connections = node.getActiveConnectionsExcept(message.sender, messageReceivedFrom);
        Task[] tasks = new Task[connections.length];
        for(int i=0;i<connections.length;i++) {
            PeerConnection connection = connections[i];
            tasks[i] = () -> {
                send(connection, message);
                return true;
            };
        }

        node.executeAllOnSendThreadPool(tasks); //ignores future, because it does not matter when the tasks finish
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

            if(knownMessageHashes.size() > 500) { //threshold to keep the remove if loop from being run always, maybe 'outsource' the clean up into a background thread
                //older than two minutes - honestly should be much more than enough to finish propagating a message
                knownMessageHashes.entrySet().removeIf(entry -> (currentTime - entry.getValue()) / 1000.0 > 60 * 2);
            }

            return oldVal != null; //there was a previous mapping
        }
    }
}