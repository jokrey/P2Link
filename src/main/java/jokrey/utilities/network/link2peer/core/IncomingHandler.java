package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
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
class IncomingHandler {
    private static final int MAX_POOL_SIZE = 256;

    DatagramSocket serverSocket;
    private P2LNodeInternal parent;

    P2LMessageQueue internalMessageQueue = new P2LMessageQueue();
    P2LMessageQueue userIdvMessageQueue = new P2LMessageQueue();
    P2LMessageQueue userBrdMessageQueue = new P2LMessageQueue();
    private final BroadcastMessageProtocol.BroadcastState broadcastState = new BroadcastMessageProtocol.BroadcastState();

    private final ThreadPoolExecutor handleReceivedMessagesPool = new ThreadPoolExecutor(8/*core size*/,MAX_POOL_SIZE/*max size*/,60, TimeUnit.SECONDS/*idle timeout*/, new LinkedBlockingQueue<>(MAX_POOL_SIZE)); // queue with a size


    private void handleReceivedMessage(DatagramPacket receivedPacket) throws IOException {
        P2LMessage message = P2LMessage.fromPacket(parent.getLinkForConnection(receivedPacket.getSocketAddress()), receivedPacket);


        if(message.type == SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //requires connection to receive data on the other side.....
            RequestPeerLinksProtocol.answerRequest(parent, receivedPacket.getSocketAddress(), parent.getActivePeerLinks());
        }


        if(message.sender == null) {
            //not connected - therefore it is a new connection

            if(message.type == SL_WHO_AM_I) {
                WhoAmIProtocol.asReceiver(parent, receivedPacket);
            } else if(message.type == SL_PEER_CONNECTION_REQUEST) {
                if(!parent.maxPeersReached()) {
                    EstablishSingleConnectionProtocol.asReceiver(parent, new InetSocketAddress(receivedPacket.getAddress(), receivedPacket.getPort()), message);
                }
            }
        } else {
            if(message.type == SC_BROADCAST) {
                P2LMessage received = BroadcastMessageProtocol.receive(parent, broadcastState, receivedPacket.getSocketAddress(), message);
                if(received != null) {
                    userBrdMessageQueue.handleNewMessage(received);
                    parent.notifyBroadcastMessageReceived(received);
                }
            } else {
                if(message.isInternalMessage()) {
                    internalMessageQueue.handleNewMessage(message);
                } else {
                    userIdvMessageQueue.handleNewMessage(message);
                    parent.notifyIndividualMessageReceived(message);
                }
            }
        }
    }

    public IncomingHandler(P2LNodeInternal parentG) throws IOException {
        this.parent = parentG;

        serverSocket = new DatagramSocket(parent.getSelfLink().port);

        new Thread(() -> {
            while(true) {
                //fixme 4096 is a heuristic
                byte[] receiveBuffer = new byte[4096]; //receive buffer needs to be new for each run, otherwise handlereceivedmessages might get weird results - maximum safe size allegedly 512
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
}
