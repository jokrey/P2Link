package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.bitsandbytes.BitHelper;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.util.Hash;
import jokrey.utilities.network.mcnp.io.ConnectionHandler;
import jokrey.utilities.network.mcnp.io.MCNP_ConnectionIO;
import jokrey.utilities.network.mcnp.io.MCNP_ServerIO;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
        P2Link from = parent.getLinkForConnection(receivedPacket.getSocketAddress());
        byte[] data = receivedPacket.getData();
        int data_len = receivedPacket.getLength()-4;
        int cause = BitHelper.getInt32FromNBytes(data, 0);

        if(from == null) {
            //not connected - therefore it is a new connection

            if(cause == SL_PEER_CONNECTION_REQUEST) {
                if(!parent.maxPeersReached()) {
                    EstablishSingleConnectionProtocol.asReceiver(parent, new InetSocketAddress(receivedPacket.getAddress(), receivedPacket.getPort()), data, data_len);
                }
            }
        } else {
            if(cause == SC_BROADCAST) {
                P2LMessage received = BroadcastMessageProtocol.receive(parent, broadcastState, from, new Hash(Arrays.copyOfRange(data, 4, data.length)));
                if(received != null) {
                    userBrdMessageQueue.handleNewMessage(received);
                    parent.notifyBroadcastMessageReceived(received);
                }
            } else if(cause == SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS) {
                RequestPeerLinksProtocol.answerRequest(parent, from, parent.getActivePeerLinks());
            } else {
                P2LMessage message = new P2LMessage(from, cause, Arrays.copyOfRange(data, 4, 4+data_len));
                if(isInternalMessageId(cause)) {
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
                byte[] receiveBuffer = new byte[4096]; //receive buffer needs to be new for each run, otherwise handlereceivedmessages might get weird results - maximum safe size allegedly 512    - todo: ensure send packages always below this size...
                DatagramPacket receivedPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                try {
                    serverSocket.receive(receivedPacket);

                    handleReceivedMessagesPool.execute(() -> {
                        //todo - dos mitigation:: could be exploited by sending(for example) many broadcast supercauses, but not sending anything else.. (the thread would hang for 5000 ms because it waits for the data to be send...)

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
