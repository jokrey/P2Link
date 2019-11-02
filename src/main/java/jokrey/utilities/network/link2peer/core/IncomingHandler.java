package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadLocalRandom;

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
        System.out.println(parent.getPort() + " - IncomingHandler_handleReceivedMessage - from = [" + from + "], message = [" + message + "]");

        parent.notifyPacketReceivedFrom(from);

        //todo:?: allow streams and long messages only from established connections? - why tho?
        if(message.header.isStreamPart()) {
            System.out.println("received stream part message: from = [" + from + "], message = [" + message + "]");
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
        else if (message.header.getType() == SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //requires connection to asAnswerer data on the other side.....
            RequestPeerLinksProtocol.asAnswerer(parent, from);
        } else if (message.header.getType() == SL_WHO_AM_I) {
            WhoAmIProtocol.asAnswerer(parent, receivedPacket);
        } else if (message.header.getType() == SL_PING) {
            PingProtocol.asAnswerer(parent, from);
        } else if (message.header.getType() == SL_PONG) {
            //already notify packet received from called, i.e. it is no longer marked as dormant
        } else if (message.header.getType() == SL_PEER_CONNECTION_REQUEST) {
            if (!parent.connectionLimitReached()) {
                EstablishSingleConnectionProtocol.asAnswerer(parent, new InetSocketAddress(receivedPacket.getAddress(), receivedPacket.getPort()), message);
            }
        } else if (message.header.getType() == SC_BROADCAST) {
            P2LMessage received = BroadcastMessageProtocol.asAnswerer(parent, broadcastState, from, message);
            if (received != null) {
                userBrdMessageQueue.handleNewMessage(received);
                parent.notifyBroadcastMessageReceived(received);
            }
        } else if (message.header.getType() == SC_DISCONNECT) {
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
