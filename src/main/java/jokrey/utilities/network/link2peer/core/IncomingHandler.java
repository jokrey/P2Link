package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.*;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * Protocol:
 * UDP
 * Packet structure(inside udp):
 * 0-4 bytes: cause - int32 which determines the message cause/type
 * 5-(udp_len-4) bytes: data
 *
 *
 * @author jokrey
 */
public class IncomingHandler {
    public static int INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE = 0;
    private static final int MAX_POOL_SIZE = 256;

    DatagramSocket serverSocket;
    private P2LNodeInternal parent;

    P2LMessageQueue internalMessageQueue = new P2LMessageQueue();
    P2LMessageQueue userMessageQueue = new P2LMessageQueue();
    P2LMessageQueue userBrdMessageQueue = new P2LMessageQueue();
    P2LMessageQueue receiptsQueue = new P2LMessageQueue();
    final BroadcastMessageProtocol.BroadcastState broadcastState = new BroadcastMessageProtocol.BroadcastState();
//    final RetryHandler retryHandler = new RetryHandler();

//    private final ThreadPoolExecutor handleReceivedMessagesPool = new ThreadPoolExecutor(4/*core size*/, MAX_POOL_SIZE/*max size*/, 60, TimeUnit.SECONDS/*idle timeout*/, new LinkedBlockingQueue<>(MAX_POOL_SIZE * 2),
//            r -> new Thread()); // queue with a size
    private final P2LThreadPool handleReceivedMessagesPool = new P2LThreadPool(4, 64);


    private void handleReceivedMessage(DatagramPacket receivedPacket) throws IOException {
        SocketAddress from = receivedPacket.getSocketAddress();
        P2LMessage message = P2LMessage.fromPacket(receivedPacket);
        boolean dropped = ThreadLocalRandom.current().nextInt(0, 100) < INTENTIONALLY_DROPPED_PACKAGE_PERCENTAGE;
//        System.out.println((dropped?" - DROPPED - ":"") + parent.getPort()+" - IncomingHandler_handleReceivedMessage - from = [" + from + "], message = [" + message + "]");
        if(dropped) return;


//            System.out.println(parent.getPort()+" received message = " + message);
//            System.out.println(parent.getPort()+" from = " + from);

        if(message.requestReceipt) {
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
            //todo - solve: send retryCounter
        }
        if(message.isReceipt)
            receiptsQueue.handleNewMessage(message);
        else if (message.type == SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //requires connection to asAnswerer data on the other side.....
            RequestPeerLinksProtocol.asAnswerer(parent, from);
        } else if (message.type == SL_WHO_AM_I) {
            WhoAmIProtocol.asAnswerer(parent, receivedPacket);
        } else if (message.type == SL_PEER_CONNECTION_REQUEST) {
            if (!parent.connectionLimitReached()) {
                EstablishSingleConnectionProtocol.asReceiver(parent, new InetSocketAddress(receivedPacket.getAddress(), receivedPacket.getPort()), message);
            }
        } else if (message.type == SC_BROADCAST) {
            P2LMessage received = BroadcastMessageProtocol.asAnswerer(parent, broadcastState, from, message);
            if (received != null) {
                userBrdMessageQueue.handleNewMessage(received);
                parent.notifyBroadcastMessageReceived(received);
            }
        } else if (message.type == SC_DISCONNECT) {
            DisconnectSingleConnectionProtocol.asReceiver(parent, from);
        } else {
            if (message.isInternalMessage()) {
                internalMessageQueue.handleNewMessage(message);
            } else {
                userMessageQueue.handleNewMessage(message);
                parent.notifyMessageReceived(message);
            }
        }


    }

    public IncomingHandler(P2LNodeInternal parentG) throws IOException {
        this.parent = parentG;

        serverSocket = new DatagramSocket(parent.getPort());

        new Thread(() -> {
            while(true) {
                //fixme 1024 is a heuristic
                byte[] receiveBuffer = new byte[1024]; //asAnswerer buffer needs to be new for each run, otherwise handlereceivedmessages might get weird results - maximum safe size allegedly 512
                DatagramPacket receivedPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    serverSocket.receive(receivedPacket);

                    handleReceivedMessagesPool.execute(
//                    new Thread(
                        () -> {
                        //has to be on a thread, because most protocols also wait for an answer - that has to be done outside of the thread that receives the answer (the outer thread here...)
                        //    ((( DOS mitigation:: could be exploited by sending(for example) many broadcast supercauses, but not sending anything else.. (the thread would hang for 5000 ms because it waits for the data to be send...)

                        try {
                            handleReceivedMessage(receivedPacket);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    })
//                    .start()
                    ;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
