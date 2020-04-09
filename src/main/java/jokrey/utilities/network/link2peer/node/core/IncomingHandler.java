package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.conversation.ConversationHandler;
import jokrey.utilities.network.link2peer.node.conversation.ConversationHandlerV2_2;
import jokrey.utilities.network.link2peer.node.protocols.*;
import jokrey.utilities.network.link2peer.node.stream.StreamMessageHandler;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.*;

/**
 * Protocol:
 * UDP
 * Packet structure(inside udp):
 * see header
 *
 * @author jokrey
 */
public class IncomingHandler {
    DatagramSocket serverSocket;
    P2LNodeInternal parent;

    public final P2LMessageReceivalQueue messageQueue = new P2LMessageReceivalQueue();
    final P2LBroadcastMessageQueue brdMessageQueue = new P2LBroadcastMessageQueue();
    final BroadcastMessageProtocol.BroadcastState broadcastState = new BroadcastMessageProtocol.BroadcastState();
    final LongMessageHandler longMessageHandler = new LongMessageHandler();
    final StreamMessageHandler streamMessageHandler = new StreamMessageHandler();
    final ConversationHandler conversationMessageHandler = new ConversationHandlerV2_2();

    final P2LThreadPool handleReceivedMessagesPool = new P2LThreadPool(4, 64);


    private void handleReceivedMessage(DatagramPacket receivedPacket) throws Throwable {
        InetSocketAddress from = (InetSocketAddress) receivedPacket.getSocketAddress();
        ReceivedP2LMessage message = P2LMessage.fromPacket(from, receivedPacket);
        if(DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE>0) {
            boolean dropped = ThreadLocalRandom.current().nextInt(0, 100) < DebugStats.INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE;
            if (dropped) {
                System.out.println(" - DROPPED - "+parent.getSelfLink() + " - IncomingHandler_handleReceivedMessage - from = [" + from + "], message = [" + message + "]");
                DebugStats.incomingHandler_numIntentionallyDropped.getAndIncrement();
                return;
            }
        }
        if(message.header.getType() == SL_NAT_HOLE_PACKET) return; //Packets of this type are dropped ASAP
        if(DebugStats.MSG_PRINTS_ACTIVE)
            System.out.println(parent.getSelfLink() + " - handleReceivedMessage - from = [" + from + "], message = [" + message + "]");

        parent.notifyPacketReceivedFrom(from);

        if(message.header.isLongPart()) {
            message = longMessageHandler.received(message);//NOTE: does not work for steps..
            if(message == null) return; //not yet entire message received
        }

        if(message.header.isStreamPart()) {
            if(message.header.isReceipt()) {
                streamMessageHandler.receivedReceipt(message);
                DebugStats.incomingHandler_numStreamReceipts.getAndIncrement();
            } else {
                streamMessageHandler.receivedPart(parent, message);
                DebugStats.incomingHandler_numStreamParts.getAndIncrement();
            }
        } else if(message.header.isConversationPart()) {
            conversationMessageHandler.received(parent, from, message);
        } else {
            if (message.header.requestReceipt())
                parent.sendInternalMessage(from, message.createReceipt());

            if(message.header.isReceipt()) {
                messageQueue.handleNewMessage(message);
//           } else if (message.header.getType() == SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //requires connection to asAnswererDirect data on the other side.....
//                RequestPeerLinksProtocol.asAnswerer(parent, from);
//            } else if (message.header.getType() == SL_WHO_AM_I) {
//                WhoAmIProtocol.asAnswerer(parent, receivedPacket);
//            } else if (message.header.getType() == SL_PING) {
//            PingProtocol.asAnswerer(parent, from);
                //ping always requests a receipt - so that was already sent
//            } else if (message.header.getType() == SL_DIRECT_CONNECTION_REQUEST) {
//                EstablishConnectionProtocol.asAnswererDirect(parent, receivedPacket.getSocketAddress(), message);
//            } else if(message.header.getType() == SL_REQUEST_DIRECT_CONNECT_TO) {
//                EstablishConnectionProtocol.asAnswererRequestReverseConnection(parent, message);
//            } else if(message.header.getType() == SL_RELAY_REQUEST_DIRECT_CONNECT) {
//                EstablishConnectionProtocol.asAnswererRelayRequestReverseConnection(parent, message);
//            } else if(message.header.getType() == SC_BROADCAST_WITHOUT_HASH) {
//                if(parent.isConnectedTo(from))
//                    BroadcastMessageProtocol.asAnswererWithoutHash(parent, brdMessageQueue, broadcastState, from, message);
//            } else if (message.header.getType() == SC_BROADCAST_WITH_HASH) {
//                if(parent.isConnectedTo(from))
//                    BroadcastMessageProtocol.asAnswererWithHash(parent, brdMessageQueue, broadcastState, from, message);
            } else if (message.header.getType() == SC_DISCONNECT) {
                if(parent.isConnectedTo(from)) {
                    DisconnectSingleConnectionProtocol.asAnswerer(parent, from);
                }
            } else {
                messageQueue.handleNewMessage(message);
                if (!message.isInternalMessage() && !message.header.isReceipt())
                    parent.notifyUserMessageReceived(message);
            }
        }

    }

    IncomingHandler(P2LNodeInternal parentG) throws IOException {
        this.parent = parentG;

        conversationMessageHandler.registerConversationHandlerFor(SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS, (convo, no) ->
                RequestPeerLinksProtocol.asAnswerer(parent, convo));
        conversationMessageHandler.registerConversationHandlerFor(SL_WHO_AM_I, (convo, no) ->
                WhoAmIProtocol.asAnswerer(convo));
        conversationMessageHandler.registerConversationHandlerFor(SC_BROADCAST_WITHOUT_HASH, (convo, m0) -> {
            if(parent.isConnectedTo(convo.getPeer()))
                BroadcastMessageProtocol.asAnswererWithoutHash(parent, convo, m0, brdMessageQueue, broadcastState);
        });
        conversationMessageHandler.registerConversationHandlerFor(SC_BROADCAST_WITH_HASH, (convo, m0) -> {
            if(parent.isConnectedTo(convo.getPeer()))
                BroadcastMessageProtocol.asAnswererWithHash(parent, convo, m0, brdMessageQueue, broadcastState);
        });
        conversationMessageHandler.registerConversationHandlerFor(SL_REQUEST_DIRECT_CONNECT_TO, ((convo, m0) -> {
            RelayedConnectionProtocol.asAnswerer_ConnectTo(parent, convo, m0);
        }));
        conversationMessageHandler.registerConversationHandlerFor(SL_REQUEST_DIRECT_CONNECT_TO_MODIFY_DESTINATION_IP_TO_RELAY_IP, ((convo, m0) -> {
            RelayedConnectionProtocol.asAnswerer_ConnectTo_modifyDestinationIpToRelayIp(parent, convo, m0);
        }));
        conversationMessageHandler.registerConversationHandlerFor(SL_CONNECTION_RELAY, ((convo, m0) -> {
            RelayedConnectionProtocol.asAnswerer_RelayConnection(parent, convo, m0);
        }));
        conversationMessageHandler.registerConversationHandlerFor(SL_DIRECT_CONNECTION_REQUEST, ((convo, m0) -> {
            DirectConnectionProtocol.asAnswerer(parent, convo, m0);
        }));

        serverSocket = new DatagramSocket(parent.getSelfLink().getPort());
        serverSocket.setTrafficClass(0x10 | 0x08); //emphasize IPTOS_THROUGHPUT & IPTOS_LOWDELAY  - these options will likely be ignored by the underlying implementation
        serverSocket.setReceiveBufferSize(P2LMessage.CUSTOM_RAW_SIZE_LIMIT);

        new Thread(() -> {
            while(!serverSocket.isClosed()) {
                byte[] receiveBuffer = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT]; //asAnswererDirect buffer needs to be new for each run, otherwise handleReceivedMessages" might get weird results - maximum safe size allegedly 512

                DatagramPacket receivedPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    serverSocket.receive(receivedPacket);
//                    System.out.println("received packet from: "+receivedPacket.getSocketAddress());

                    handleReceivedMessagesPool.execute(() -> {
                        //has to be on a thread, because most protocols also wait for an answer - that has to be done outside of the thread that receives the answer (the outer thread here...)
                        //    ((( DOS mitigation:: could be exploited by sending(for example) many broadcast super causes, but not sending anything else.. (the thread would hang for many ms because it waits for the data to be send...)

                        try {
                            handleReceivedMessage(receivedPacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (SocketException e) {
                    if(e.getMessage().equals("socket closed"))
                        return;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void close() {
        serverSocket.close();
        handleReceivedMessagesPool.shutdown();
        messageQueue.clear();
        brdMessageQueue.clear();
        broadcastState.clear();
    }
    boolean isClosed() {
        return serverSocket.isClosed();
    }
}
