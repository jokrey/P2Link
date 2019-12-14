package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LMessageQueue;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.Hash;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.*;

/**
 * @author jokrey
 */
public class BroadcastMessageProtocol {
    private static void asInitiator(P2LNodeInternal parent, P2LMessage broadcastMessage, P2LConnection con, SocketAddress to) throws IOException {
        int threshold = con==null?P2LHeuristics.BROADCAST_USES_HASH_DETOUR_RAW_SIZE_THRESHOLD:con.remoteBufferSize;
        if(broadcastMessage.requiredRawSize() <= threshold) {
            asInitiatorWithoutHash(parent, broadcastMessage, to);
        } else {
            asInitiatorWithHash(parent, broadcastMessage, to);
        }
    }

    private static void asInitiatorWithoutHash(P2LNodeInternal parent, P2LMessage message, SocketAddress to) throws IOException {
        P2LConversation convo = parent.internalConvo(SC_BROADCAST_WITHOUT_HASH, to);
        byte[] packed = packBroadcastMessage(message);
//        System.err.println("ini packed = " + Arrays.toString(packed));
        convo.initClose(packed);
    }
    public static void asAnswererWithoutHash(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0, P2LMessageQueue userBrdMessageQueue, BroadcastState state) throws IOException {
        convo.close();

//        System.err.println("ans packed = " + Arrays.toString(m0.asBytes()));
        P2LMessage receivedBroadcastMessage = unpackBroadcastMessage(m0);
        if(receivedBroadcastMessage==null ||
                state.markAsKnown(receivedBroadcastMessage.getContentHash())) //if message invalid or message was known
            return;
        relayBroadcast(parent, receivedBroadcastMessage, convo.getPeer());

        if(receivedBroadcastMessage.isInternalMessage()) {
            System.err.println("someone managed to send an internal broadcast message...? How? And more importantly why?");
        } else {
            userBrdMessageQueue.handleNewMessage(receivedBroadcastMessage);
            parent.notifyUserBroadcastMessageReceived(receivedBroadcastMessage);
        }
    }

    private static void asInitiatorWithHash(P2LNodeInternal parent, P2LMessage broadcastMessage, SocketAddress to) throws IOException {
        P2LConversation convo = parent.internalConvo(SC_BROADCAST_WITH_HASH, to);
        boolean peerHashKnowledgeOfMessage = convo.initExpectMsg(broadcastMessage.getContentHash().raw()).nextBool();
        if(!peerHashKnowledgeOfMessage) {
            convo.answerClose(packBroadcastMessage(broadcastMessage));
        } else {
            convo.close();
        }
//        return parent.tryComplete(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
//                P2LFuture.before(() ->  parent.sendInternalMessage(to, P2LMessage.Factory.createSendMessage(SC_BROADCAST_WITH_HASH, message.getContentHash().raw())),
//                                        parent.expectInternalMessage(to, C_BROADCAST_HASH_KNOWLEDGE_ANSWER))
//                .andThen(peerHashKnowledgeOfMessage_msg -> {
//                    boolean peerHashKnowledgeOfMessage = peerHashKnowledgeOfMessage_msg.nextBool();
//
//                    if(!peerHashKnowledgeOfMessage) {
////                        try {
////                            parent.sendInternalMessage(
////                                    to, packBroadcastMessage(message)
////                            );
////                        } catch (IOException e) {
////                            return new P2LFuture<>(false);
////                        }
//                    }
//                    return new P2LFuture<>(true);
//                })
//        );
    }

    public static void asAnswererWithHash(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0, P2LMessageQueue userBrdMessageQueue, BroadcastState state) throws IOException {
        Hash brdMessageHash = new Hash(m0.asBytes());
        if(state.isKnown(brdMessageHash)) {
            convo.answerClose(new byte[] {1}); //indicates true
        } else {
            P2LMessage raw = convo.answerExpectMsg(new byte[] {0}); //indicates false
            P2LMessage receivedBroadcastMessage = unpackBroadcastMessage(raw);
            if(receivedBroadcastMessage == null || state.markAsKnown(brdMessageHash)) ////if message invalid or message was known - while receiving this message, this node has received it from somewhere else
                return;

            relayBroadcast(parent, receivedBroadcastMessage, convo.getPeer());

            if(receivedBroadcastMessage.isInternalMessage()) {
                System.err.println("someone managed to send an internal broadcast message...? How? And more importantly why?");
            } else {
                userBrdMessageQueue.handleNewMessage(receivedBroadcastMessage);
                parent.notifyUserBroadcastMessageReceived(receivedBroadcastMessage);
            }
        }

//        Hash brdMessageHash = new Hash(initialMessage.asBytes());
//
////        boolean wasKnown = state.markAsKnown(brdMessageHash); //problem: if the actual message is later dropped, we never receive it at all... so we try to get the message as often as we can, but only once present it to the user
//
//        if(state.isKnown(brdMessageHash)) {
////        if(wasKnown) {
//            parent.sendInternalMessage(from, P2LMessage.Factory.createSendMessage(C_BROADCAST_HASH_KNOWLEDGE_ANSWER, true));
//            //do not tell application about broadcast again
//        } else {
//            try {
//                P2LMessage message = P2LFuture.before(
//                        ()-> parent.sendInternalMessage(from, P2LMessage.Factory.createSendMessage(C_BROADCAST_HASH_KNOWLEDGE_ANSWER, false)),
//                        parent.expectInternalMessage(from, C_BROADCAST_MSG))
//                        .get(P2LHeuristics.DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT);
//                P2LMessage receivedBroadcastMessage = unpackBroadcastMessage(message);
//                if(receivedBroadcastMessage == null || state.markAsKnown(brdMessageHash)) ////if message invalid or message was known - while receiving this message, this node has received it from somewhere else
//                    return;
//
//                relayBroadcast(parent, receivedBroadcastMessage, from);
//
//                if(receivedBroadcastMessage.isInternalMessage()) {
//                    System.err.println("someone managed to send an internal broadcast message...? How? And more importantly why?");
//                } else {
//                    userBrdMessageQueue.handleNewMessage(receivedBroadcastMessage);
//                    parent.notifyUserBroadcastMessageReceived(receivedBroadcastMessage);
//                }
//            } catch (Throwable t) {
//                t.printStackTrace();
//                state.markAsUnknown(brdMessageHash);
//            }
//
//        }
    }


    public static P2LFuture<Integer> relayBroadcast(P2LNodeInternal parent, P2LMessage message) {
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
            tasks[i] = () -> asInitiator(parent, message, parent.getConnection(connection.getSocketAddress()), connection.getSocketAddress());
        }

        return parent.executeAllOnSendThreadPool(tasks); //required, because send also waits for a response...
    }


    //the broadcast algorithm requires the message to be packed before sending - this is required to allow header information such as type and expiresAfter to differ from the carrying message
    //    additionally the sender needs to be explicitly stored - as it is omitted/automatically determined in normal messages
    private static byte[] packBroadcastMessage(P2LMessage broadcastMessage) {
        byte[] senderBytes = broadcastMessage.header.getSender().getBytesRepresentation();

        //TODO instead of going the long way around use libae - and allow libae to be a short, long, int etc en/decoder (optionally and next to the variable stuff)
        return P2LMessage.Factory.createSendMessageWith(0,
                broadcastMessage.header.getType(), broadcastMessage.header.getExpiresAfter(),
                P2LMessage.makeVariableIndicatorFor(senderBytes.length), senderBytes,
                P2LMessage.makeVariableIndicatorFor(broadcastMessage.getPayloadLength()), broadcastMessage.asBytes()).asBytes();
    }
    private static P2LMessage unpackBroadcastMessage(P2LMessage packedMessage) {
        short brdMsgType = packedMessage.nextShort();
        short expiresAfter = packedMessage.nextShort();
        P2Link sender = P2Link.fromString(packedMessage.nextVariableString());
        byte[] data = packedMessage.nextVariable();
        if(sender == null || data == null) return null;
        return P2LMessage.Factory.createBroadcast(sender, brdMsgType, expiresAfter, data);
    }

    public static class BroadcastState {
        private ConcurrentHashMap<Hash, Long> knownMessageHashes = new ConcurrentHashMap<>();
        boolean isKnown(Hash hash) {
            return knownMessageHashes.containsKey(hash);
        }
        //return if it was previously known
        public boolean markAsKnown(Hash hash) {
            long currentTime = System.currentTimeMillis();
            Long oldVal = knownMessageHashes.put(hash, currentTime);

            clean(false);//only cleans when it gets bad or it has

            return oldVal != null; //there was a previous mapping - i.e. the message was previously known
        }

        void markAsUnknown(Hash hash) {
            knownMessageHashes.remove(hash);
        }


        long lastClean = -1;
        public void clean(boolean considerTime) {
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