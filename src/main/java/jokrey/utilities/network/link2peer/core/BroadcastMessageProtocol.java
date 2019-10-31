package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.Hash;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * @author jokrey
 */
class BroadcastMessageProtocol {
    private static void asInitiator(P2LNodeInternal parent, P2LMessage message, SocketAddress to) throws IOException {
        parent.tryComplete(3, 500, () ->
                parent.expectInternalMessage(to, C_BROADCAST_MSG_KNOWLEDGE_RETURN)
                .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SC_BROADCAST, message.getContentHash().raw()), to))
                .combine(peerHashKnowledgeOfMessage_msg -> {
                    boolean peerHashKnowledgeOfMessage = peerHashKnowledgeOfMessage_msg.nextBool();

                    if(!peerHashKnowledgeOfMessage) {
                        byte[] senderBytes = message.sender.getBytes(StandardCharsets.UTF_8);
                        try {
                            parent.sendInternalMessage(
                                    P2LMessage.Factory.createSendMessageFromWithExpiration(C_BROADCAST_MSG, P2LMessage.EXPIRE_INSTANTLY, message.type,
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
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(C_BROADCAST_MSG_KNOWLEDGE_RETURN, P2LMessage.EXPIRE_INSTANTLY, true), from);
//            System.out.println("receiving message at " + parent.getSelfLink() + " - known - send msg knowledge return");
            return null; //do not tell application about broadcast again
        } else {
            try {
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - NOT known - sending msg knowledge return - hash: "+ Arrays.toString(initialMessage.asBytes()));
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - NOT known - send msg knowledge return");

                P2LMessage message = parent.expectInternalMessage(from, C_BROADCAST_MSG)
                        .nowOrCancel(()-> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(C_BROADCAST_MSG_KNOWLEDGE_RETURN, P2LMessage.EXPIRE_INSTANTLY, false), from))
                        .get(2500);
                int brdMsgType = message.nextInt();
                String sender = message.nextVariableString();
                byte[] data = message.nextVariable();
    //            System.out.println("receiving message at " + parent.getSelfLink() + " - read data");
                if(sender == null || data == null) {
                    state.markAsUnknown(brdMessageHash);
                    return null;
                }

                if(state.markAsKnown(brdMessageHash)) // while receiving this message, this node has received it from somewhere else
                    return null;

                P2LMessage receivedMessage = P2LMessage.Factory.createBroadcast(sender, brdMsgType, data);
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
        SocketAddress[] originallyEstablishedConnections = parent.getEstablishedConnections().toArray(new SocketAddress[0]);

        ArrayList<SocketAddress> establishedConnectionsExcept = new ArrayList<>(originallyEstablishedConnections.length);
        for(SocketAddress established:originallyEstablishedConnections)
            if(!WhoAmIProtocol.toString(established).equals(message.sender) && !Objects.equals(established, directlyReceivedFrom))
                establishedConnectionsExcept.add(established);

        if(establishedConnectionsExcept.isEmpty())
            return new P2LFuture<>(new Integer(0));

        P2LThreadPool.Task[] tasks = new P2LThreadPool.Task[establishedConnectionsExcept.size()];
        for (int i = 0; i < establishedConnectionsExcept.size(); i++) {
            SocketAddress connection = establishedConnectionsExcept.get(i);
            tasks[i] = () -> asInitiator(parent, message, connection);
        }

        return parent.executeAllOnSendThreadPool(tasks); //required, because send also waits for a response...

        //TODO: 'short' broadcasts that do not do the hash thing (in that case it is not required to wait for a response...)
        //TODO: evolve the protocol into a protocol that breaks up very long messages into multiple udp packages...
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

            if(knownMessageHashes.size() > 500) { //threshold to keep the remove if loop from being run always, maybe 'outsource' the clean up into a background thread
                //older than two minutes - honestly should be much more than enough to finish propagating a message
                clean();
            }

            return oldVal != null; //there was a previous mapping - i.e. the message was previously known
        }

        void markAsUnknown(Hash hash) {
            knownMessageHashes.remove(hash);
        }


        //FIXME - all of these are barely researched heuristics
        long lastClean = -1;
        void clean() {
            long currentTime = System.currentTimeMillis();
            if(lastClean==-1 || (currentTime - lastClean) / 1000.0 > 30 || knownMessageHashes.size() > 3500) {//only clean every thirty seconds, otherwise it might be spammed
                lastClean=currentTime;
                knownMessageHashes.entrySet().removeIf(entry -> (currentTime - entry.getValue()) / 1000.0 > 60 * 2);
                //removes all hashes older than 2 minute - i.e. the same broadcast can be send every roughly 2 minutes
                //also means that a broadcast has to pass through(and fade, i.e. no longer redistributed) the network within 2 minutes
            }
        }

        public void clear() {
            knownMessageHashes.clear();
        }
    }
}