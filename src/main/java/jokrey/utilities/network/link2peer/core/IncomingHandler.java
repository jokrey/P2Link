package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

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

    DatagramSocket serverSocket;
    private P2LNodeInternal parent;

    final P2LMessageQueue internalMessageQueue = new P2LMessageQueue();
    final P2LMessageQueue userMessageQueue = new P2LMessageQueue();
    final P2LMessageQueue userBrdMessageQueue = new P2LMessageQueue();
    final P2LMessageQueue receiptsQueue = new P2LMessageQueue();
    final BroadcastMessageProtocol.BroadcastState broadcastState = new BroadcastMessageProtocol.BroadcastState();
    final LongMessageHandler longMessageHandler = new LongMessageHandler();
//    final RetryHandler retryHandler = new RetryHandler();

    final P2LThreadPool handleReceivedMessagesPool = new P2LThreadPool(4, 64);


    private void handleReceivedMessage(DatagramPacket receivedPacket) throws Throwable {
        SocketAddress from = receivedPacket.getSocketAddress();
        P2LMessage message = P2LMessage.fromPacket(receivedPacket);
        if(INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE>0) {
            boolean dropped = ThreadLocalRandom.current().nextInt(0, 100) < INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE;
            if (dropped) {
                System.out.print(" - DROPPED - ");
                System.out.println(parent.getPort() + " - IncomingHandler_handleReceivedMessage - from = [" + from + "], message = [" + message + "]");
                return;
            }
        }

        parent.notifyPacketReceivedFrom(from);

        if(message.isLongPart) {
            message = longMessageHandler.received(message.toLongMessagePart());
            if(message == null) return; //not yet entire message received
        }

        if (message.requestReceipt) {
            //TODO - problem: double send receipt - i.e.
//            if(message.isRetry) {
//                boolean hasBeenHandled = retryHandler.hasBeenHandled(message);
//                if(hasBeenHandled)
//                    return;
//            } else {
//                retryHandler.markHandled(message);
            parent.sendInternalMessage(P2LMessage.createReceiptFor(message), from);
//            }
            //todo - what if this packet is lost? then the client will resend, and potentially redo this operation
            //todo - solve: send retryCounter (or rather conversation id)
        }
        if (message.isReceipt)
            receiptsQueue.handleNewMessage(message);
        else if (message.type == SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //requires connection to asAnswerer data on the other side.....
            RequestPeerLinksProtocol.asAnswerer(parent, from);
        } else if (message.type == SL_WHO_AM_I) {
            WhoAmIProtocol.asAnswerer(parent, receivedPacket);
        } else if (message.type == SL_PING) {
            PingProtocol.asAnswerer(parent, from);
        } else if (message.type == SL_PONG) {
            //already notify packet received from called, i.e. it is no longer marked as dormant
        } else if (message.type == SL_PEER_CONNECTION_REQUEST) {
            if (!parent.connectionLimitReached()) {
                EstablishSingleConnectionProtocol.asAnswerer(parent, new InetSocketAddress(receivedPacket.getAddress(), receivedPacket.getPort()), message);
            }
        } else if (message.type == SC_BROADCAST) {
            P2LMessage received = BroadcastMessageProtocol.asAnswerer(parent, broadcastState, from, message);
            if (received != null) {
                userBrdMessageQueue.handleNewMessage(received);
                parent.notifyBroadcastMessageReceived(received);
            }
        } else if (message.type == SC_DISCONNECT) {
            DisconnectSingleConnectionProtocol.asAnswerer(parent, from);
        } else {
            if (message.isInternalMessage()) {
                internalMessageQueue.handleNewMessage(message);
            } else {
                userMessageQueue.handleNewMessage(message);
                parent.notifyMessageReceived(message);
            }
        }

    }

    IncomingHandler(P2LNodeInternal parentG) throws IOException {
        this.parent = parentG;

        serverSocket = new DatagramSocket(parent.getPort());

        new Thread(() -> {
            while(!serverSocket.isClosed()) {
                byte[] receiveBuffer = new byte[P2LMessage.CUSTOM_RAW_SIZE_LIMIT]; //asAnswerer buffer needs to be new for each run, otherwise handlereceivedmessages might get weird results - maximum safe size allegedly 512
                DatagramPacket receivedPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    serverSocket.receive(receivedPacket);

                    handleReceivedMessagesPool.execute(() -> {
                        //has to be on a thread, because most protocols also wait for an answer - that has to be done outside of the thread that receives the answer (the outer thread here...)
                        //    ((( DOS mitigation:: could be exploited by sending(for example) many broadcast supercauses, but not sending anything else.. (the thread would hang for 5000 ms because it waits for the data to be send...)

                        try {
                            handleReceivedMessage(receivedPacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
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
