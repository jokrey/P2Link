package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.core.OutgoingHandler.Task;
import jokrey.utilities.network.link2peer.util.Hash;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * @author jokrey
 */
class BroadcastMessageProtocol {
    public static boolean send(P2LNodeInternal parent, P2Link peer, int msgId, byte[] message) {
        try {
            send(parent, peer, new P2LMessage(parent.getSelfLink(), msgId, message));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static void send(P2LNodeInternal parent, P2Link peer, P2LMessage message) throws IOException {
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        parent.sendRaw(new P2LMessage(parent.getSelfLink(), SC_BROADCAST, message.getHash().raw()), peerConnection);

        P2LMessage peerHashKnowledgeOfMessage_msg = parent.futureForInternal(peer, C_BROADCAST_MSG_KNOWLEDGE_RETURN).get(5000);
        boolean peerHashKnowledgeOfMessage = peerHashKnowledgeOfMessage_msg.asBool();

        //System.out.println("sending brd to "+connection.peerLink + " - peer has knowledge of message received - "+peerHashKnowledgeOfMessage);

        if(!peerHashKnowledgeOfMessage) {
            parent.sendRaw(
                    new P2LMessage(
                        parent.getSelfLink(), C_BROADCAST_MSG,
                        new LIbae().encode(P2LMessage.fromInt(message.type), message.sender.getRepresentingByteArray(), message.data).getEncoded()),
                    peerConnection);
            //System.out.println("sending brd to "+connection.peerLink + " - send sender + data");
        }
    }

    public static P2LMessage receive(P2LNodeInternal parent, BroadcastState state, P2Link peer, Hash broadcastMessageHash) throws IOException {
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        //System.out.println("receiving message at " + selfNode.getSelfLink());

        boolean wasKnown = state.markAsKnown(broadcastMessageHash);

        //System.out.println("receiving message at " + selfNode.getSelfLink() + " - got hash");

        if(wasKnown) {
//        if(state.isKnown(hash)) {
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - known - sending msg knowledge return");
            parent.sendRaw(new P2LMessage(parent.getSelfLink(), C_BROADCAST_MSG_KNOWLEDGE_RETURN, P2LMessage.fromBool(true)), peerConnection);
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - known - send msg knowledge return");
            return null; //do not tell application over broadcast again
        } else {
//            state.markAsKnown(hash);

            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - NOT known - sending msg knowledge return");
            parent.sendRaw(new P2LMessage(parent.getSelfLink(), C_BROADCAST_MSG_KNOWLEDGE_RETURN, P2LMessage.fromBool(false)), peerConnection);
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - NOT known - send msg knowledge return");

            Iterator<byte[]> libae = new LIbae(parent.futureForInternal(peer, C_BROADCAST_MSG).get(5000).data).iterator();
            int id = P2LMessage.trans.detransform_int(libae.next());
            P2Link sender = new P2Link(P2LMessage.trans.detransform_string(libae.next()));
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - read sender");
            byte[] data = libae.next();
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - read data");
            P2LMessage receivedMessage = new P2LMessage(sender, id, data);

            relayBroadcast(receivedMessage, parent, peer);
            //System.out.println("receiving message at " + selfNode.getSelfLink() + " - relayed broadcast");

            //todo mark as unknown on error...

            return receivedMessage;
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
        node.executeAllOnSendThreadPool(tasks);
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