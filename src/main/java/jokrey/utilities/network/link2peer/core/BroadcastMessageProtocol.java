package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.Hash;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.*;

/**
 * @author jokrey
 */
class BroadcastMessageProtocol {
    private static void asInitiator(P2LNodeInternal parent, P2LMessage message, SocketAddress to) throws IOException {
        parent.tryComplete(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
                parent.expectInternalMessage(to, C_BROADCAST_MSG_KNOWLEDGE_RETURN)
                .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SC_BROADCAST, message.getContentHash().raw()), to))
                .combine(peerHashKnowledgeOfMessage_msg -> {
                    boolean peerHashKnowledgeOfMessage = peerHashKnowledgeOfMessage_msg.nextBool();

                    if(!peerHashKnowledgeOfMessage) {
                        byte[] senderBytes = message.header.getSender().getBytesRepresentation();
                        try {
                            parent.sendInternalMessage(
                                    P2LMessage.Factory.createSendMessageWith(C_BROADCAST_MSG,
                                            message.header.getType(), message.header.getExpiresAfter(),
                                            P2LMessage.makeVariableIndicatorFor(senderBytes.length), senderBytes,
                                            P2LMessage.makeVariableIndicatorFor(message.payloadLength), message.asBytes()),
                                    to);
                        } catch (IOException e) {
                            return new P2LFuture<>(false);
                        }
                    }
                    return new P2LFuture<>(true);
                })
        );
    }

    static P2LMessage asAnswerer(P2LNodeInternal parent, BroadcastState state, SocketAddress from, P2LMessage initialMessage) throws IOException {
//        System.out.println("receiving message at " + parent.getSelfLink());

        Hash brdMessageHash = new Hash(initialMessage.asBytes());

//        System.out.println("receiving message at " + parent.getSelfLink() + " - got hash");
//        boolean wasKnown = state.markAsKnown(brdMessageHash); //problem: if the actual message is later dropped, we never receive it at all... so we try to get the message as often as we can, but only once present it to the user

        if(state.isKnown(brdMessageHash)) {
//        if(wasKnown) {
//            System.out.println("receiving message at " + parent.getSelfLink() + " - known - sending msg knowledge return");
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(C_BROADCAST_MSG_KNOWLEDGE_RETURN, true), from);
//            System.out.println("receiving message at " + parent.getSelfLink() + " - known - send msg knowledge return");
            return null; //do not tell application about broadcast again
        } else {
            try {
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - NOT known - sending msg knowledge return - hash: "+ Arrays.toString(initialMessage.asBytes()));
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - NOT known - send msg knowledge return");

                P2LMessage message = parent.expectInternalMessage(from, C_BROADCAST_MSG)
                        .nowOrCancel(()-> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(C_BROADCAST_MSG_KNOWLEDGE_RETURN, false), from))
                        .get(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT);
                int brdMsgType = message.nextInt();
                short expiresAfter = message.nextShort();
                P2Link sender = P2Link.fromString(message.nextVariableString());
                byte[] data = message.nextVariable();
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - read data");
                if(sender == null || data == null) {
                    state.markAsUnknown(brdMessageHash);
                    return null;
                }

                if(state.markAsKnown(brdMessageHash)) // while receiving this message, this node has received it from somewhere else
                    return null;

                P2LMessage receivedMessage = P2LMessage.Factory.createBroadcast(sender, brdMsgType, expiresAfter, data);
                relayBroadcast(parent, receivedMessage, from);
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - relayed broadcast");

                return receivedMessage;
            } catch (Throwable t) {
                t.printStackTrace();
                state.markAsUnknown(brdMessageHash);
                return null;
            }

        }
    }

    static P2LFuture<Integer> relayBroadcast(P2LNodeInternal parent, P2LMessage message) {
        return relayBroadcast(parent, message, null);
    }
    private static P2LFuture<Integer> relayBroadcast(P2LNodeInternal parent, P2LMessage message, SocketAddress directlyReceivedFrom) {
        P2Link[] originallyEstablishedConnections = parent.getEstablishedConnections().toArray(new P2Link[0]);

        ArrayList<P2Link> establishedConnectionsExcept = new ArrayList<>(originallyEstablishedConnections.length);
        for(P2Link established:originallyEstablishedConnections)
            if(!established.equals(message.header.getSender()) && !Objects.equals(established.getSocketAddress(), directlyReceivedFrom))
                establishedConnectionsExcept.add(established);

        if(establishedConnectionsExcept.isEmpty())
            return new P2LFuture<>(new Integer(0));

        P2LThreadPool.Task[] tasks = new P2LThreadPool.Task[establishedConnectionsExcept.size()];
        for (int i = 0; i < establishedConnectionsExcept.size(); i++) {
            P2Link connection = establishedConnectionsExcept.get(i);
            tasks[i] = () -> asInitiator(parent, message, connection.getSocketAddress());
        }

        return parent.executeAllOnSendThreadPool(tasks); //required, because send also waits for a response...

        //TODO: 'short' broadcasts that do not do the hash thing (in that case it is not required to wait for a response...)
    }


    static class BroadcastState {
        private ConcurrentHashMap<Hash, Long> knownMessageHashes = new ConcurrentHashMap<>();
        boolean isKnown(Hash hash) {
            return knownMessageHashes.containsKey(hash);
        }
        //return if it was previously known
        boolean markAsKnown(Hash hash) {
            long currentTime = System.currentTimeMillis();
            Long oldVal = knownMessageHashes.put(hash, currentTime);

            clean(false);//only cleans when it gets bad or it has

            return oldVal != null; //there was a previous mapping - i.e. the message was previously known
        }

        void markAsUnknown(Hash hash) {
            knownMessageHashes.remove(hash);
        }


        long lastClean = -1;
        void clean(boolean considerTime) {
            long currentTime = System.currentTimeMillis();
            if((considerTime && (lastClean==-1 || (currentTime - lastClean) > P2LHeuristics.BROADCAST_STATE_ATTEMPT_CLEAN_TIMEOUT_TRIGGER_MS))
                    || knownMessageHashes.size() > P2LHeuristics.BROADCAST_STATE_ATTEMPT_CLEAN_KNOWN_HASH_COUNT_TRIGGER) {//only clean every thirty seconds, otherwise it might be spammed
                lastClean=currentTime;
                knownMessageHashes.entrySet().removeIf(entry -> (currentTime - entry.getValue()) > P2LHeuristics.BROADCAST_STATE_KNOWN_HASH_TIMEOUT_MS);
                //removes all hashes older than 2 minute - i.e. the same broadcast can be send every roughly 2 minutes
                //also means that a broadcast has to pass through(and fade, i.e. no longer redistributed) the network within 2 minutes
            }
        }

        public void clear() {
            knownMessageHashes.clear();
        }

        public String debugString() {
            return knownMessageHashes.toString();
        }
    }
}