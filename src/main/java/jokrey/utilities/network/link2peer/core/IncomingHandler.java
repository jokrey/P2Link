package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.stream.StreamMessageHandler;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.*;

/**
 * Protocol:
 * UDP
 * Packet structure(inside udp):
 * see header
 *
 * @author jokrey
 */
public class IncomingHandler {
    public static int INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
    public static AtomicInteger NUMBER_OF_STREAM_RECEIPTS_RECEIVED = new AtomicInteger(0);
    public static AtomicInteger NUMBER_OF_STREAM_PARTS_RECEIVED = new AtomicInteger(0);

    DatagramSocket serverSocket;
    private P2LNodeInternal parent;

    final P2LMessageQueue internalMessageQueue = new P2LMessageQueue();
    final P2LMessageQueue userMessageQueue = new P2LMessageQueue();
    final P2LMessageQueue userBrdMessageQueue = new P2LMessageQueue();
    final P2LMessageQueue receiptsQueue = new P2LMessageQueue();
    final BroadcastMessageProtocol.BroadcastState broadcastState = new BroadcastMessageProtocol.BroadcastState();
    final LongMessageHandler longMessageHandler = new LongMessageHandler();
    final StreamMessageHandler streamMessageHandler = new StreamMessageHandler();
//    final RetryHandler retryHandler = new RetryHandler();

    final P2LThreadPool handleReceivedMessagesPool = new P2LThreadPool(4, 64);


    private void handleReceivedMessage(DatagramPacket receivedPacket) throws Throwable {
        SocketAddress from = receivedPacket.getSocketAddress();
        P2LMessage message = P2LMessage.fromPacket(receivedPacket);
        if(INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE>0) {
            boolean dropped = ThreadLocalRandom.current().nextInt(0, 100) < INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE;
            if (dropped) {
                System.out.print(" - DROPPED - ");
                System.out.println(parent.getSelfLink() + " - IncomingHandler_handleReceivedMessage - from = [" + from + "], message = [" + message + "]");
                return;
            }
        }
        System.out.println(parent.getSelfLink() + " - IncomingHandler_handleReceivedMessage - from = [" + from + "], message = [" + message + "]");

        parent.notifyPacketReceivedFrom(from);

        //todo:?: allow streams and long messages only from established connections? - why tho? - mtu knowledge + some more ddos protection maybe
        if(message.header.isStreamPart()) {
            if(message.header.isReceipt()) {
                streamMessageHandler.receivedReceipt(message);
                NUMBER_OF_STREAM_RECEIPTS_RECEIVED.getAndIncrement();
            } else {
                streamMessageHandler.receivedPart(message);
                NUMBER_OF_STREAM_PARTS_RECEIVED.getAndIncrement();
            }
            return;
        } else if(message.header.isLongPart()) {
            message = longMessageHandler.received(message);
            if(message == null) return; //not yet entire message received
        }

        if (message.header.requestReceipt()) {
            //TODO - problem: double send of message, after receipt packet was lost
            //todo -   ends in message being handled twice (conversation id likely different, but message has same semantics)
//            if(message.isRetry) {
//                boolean hasBeenHandled = retryHandler.hasBeenHandled(message);
//                if(hasBeenHandled)
//                    return;
//            } else {
//                retryHandler.markHandled(message);
            parent.sendInternalMessage(message.createReceipt(), from);
//            }
        }
        if (message.header.isReceipt())
            receiptsQueue.handleNewMessage(message);
        else if (message.header.getType() == SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //requires connection to asAnswererDirect data on the other side.....
            RequestPeerLinksProtocol.asAnswerer(parent, from);
        } else if (message.header.getType() == SL_WHO_AM_I) {
            WhoAmIProtocol.asAnswerer(parent, receivedPacket);
        } else if (message.header.getType() == SL_PING) {
            PingProtocol.asAnswerer(parent, from);
        } else if (message.header.getType() == SL_PONG) {
            //already 'notify packet received from' called, i.e. it is no longer marked as dormant
        } else if (message.header.getType() == SL_DIRECT_CONNECTION_REQUEST) {
            if (!parent.connectionLimitReached()) {
                EstablishConnectionProtocol.asAnswererDirect(parent, receivedPacket.getSocketAddress(), message);
            }
        } else if(message.header.getType() == SL_REQUEST_DIRECT_CONNECT_TO) {
            if (!parent.connectionLimitReached()) {
                EstablishConnectionProtocol.asAnswererRequestReverseConnection(parent, message);
            }
        } else if(message.header.getType() == SL_RELAY_REQUEST_DIRECT_CONNECT) {
            EstablishConnectionProtocol.asAnswererRelayRequestReverseConnection(parent, message);
        } else if(message.header.getType() == SC_BROADCAST_WITHOUT_HASH) {
            P2LMessage received = BroadcastMessageProtocol.asAnswererWithoutHash(parent, broadcastState, from, message);
            if (received != null) {
                if(received.isInternalMessage()) {
                    System.err.println("someone managed to send an internal broadcast message...? How? And more importantly why?");
                } else {
                    userBrdMessageQueue.handleNewMessage(received);
                    parent.notifyUserBroadcastMessageReceived(received);
                }
            }
        } else if (message.header.getType() == SC_BROADCAST_WITH_HASH) {
            P2LMessage received = BroadcastMessageProtocol.asAnswererWithHash(parent, broadcastState, from, message);
            if (received != null) {
                if(received.isInternalMessage()) {
                    System.err.println("someone managed to send an internal broadcast message...? How? And more importantly why?");
                } else {
                    userBrdMessageQueue.handleNewMessage(received);
                    parent.notifyUserBroadcastMessageReceived(received);
                }
            }
        } else if (message.header.getType() == SC_DISCONNECT) {
            DisconnectSingleConnectionProtocol.asAnswerer(parent, from);
        } else {
            if (message.isInternalMessage()) {
                internalMessageQueue.handleNewMessage(message);
            } else {
                userMessageQueue.handleNewMessage(message);
                parent.notifyUserMessageReceived(message);
            }
        }

    }

    IncomingHandler(P2LNodeInternal parentG) throws IOException {
        this.parent = parentG;

        serverSocket = new DatagramSocket(parent.getSelfLink().getPort());
        serverSocket.setTrafficClass(0x10 | 0x08); //emphasize IPTOS_THROUGHPUT & IPTOS_LOWDELAY  - this option will likely be ignored by the underlying implementation
        serverSocket.setReceiveBufferSize(P2LMessage.CUSTOM_RAW_SIZE_LIMIT);

        new Thread(() -> {
            while(!serverSocket.isClosed()) {
                byte[] receiveBuffer = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT]; //asAnswererDirect buffer needs to be new for each run, otherwise handleReceivedMessages" might get weird results - maximum safe size allegedly 512

                DatagramPacket receivedPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    serverSocket.receive(receivedPacket);

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
        internalMessageQueue.clear();
        userMessageQueue.clear();
        userBrdMessageQueue.clear();
        receiptsQueue.clear();
        broadcastState.clear();
    }
    boolean isClosed() {
        return serverSocket.isClosed();
    }
}
