package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class EstablishSingleConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    static boolean asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.asInitiator(parent, link, outgoing);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }

        boolean success = parent.tryReceive(3, 500, () -> {
            if(parent.isConnectedTo(to)) return new P2LFuture<>(false);
            int conversationId = parent.createUniqueConversationId();
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PEER_CONNECTION_REQUEST, conversationId), to);
            return parent.expectInternalMessage(to, R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId).combine(message -> {
                byte[] verifyNonce = message.asBytes();
                if(verifyNonce.length==0) return new P2LFuture<>(false);
                if(verifyNonce.length==1) {
                    return new P2LFuture<>(true);
                }
                try {
                    parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId, verifyNonce), to);
                    return parent.expectInternalMessage(to, R_CONNECTION_ESTABLISHED, conversationId).toBooleanFuture(m->true);
                } catch (IOException e) {
                    e.printStackTrace();
                    return new P2LFuture<>(null);//causes retry!!
                }
            });
        });

        if(success)
            parent.graduateToEstablishedConnection(to);
        return success;
    }

    static void asReceiver(P2LNodeInternal parent, InetSocketAddress from, P2LMessage initialRequestMessage) throws IOException {
//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.asInitiator(parent, link, outgoing);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }

        int conversationId = initialRequestMessage.conversationId;
        if(parent.connectionLimitReached()) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId), from); //do not retry refusal
            return;
        }
        if(parent.isConnectedTo(from)) {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, new byte[1]), from); //do not retry refusal
            return;
        }


        // send a nonce to other peer
        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list - or they are )
        byte[] nonce = new byte[16];
        secureRandom.nextBytes(nonce);
        //next line blocking, due to the following problem: if the packet is lost, the initiator will retry the connection:
        //     sending a new connection-request + receiving a verify nonce request WITH A DIFFERENT NONCE - however, since this came first, we will receive the nonce here...
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, conversationId, nonce), from);

        byte[] verifyNonce = parent.expectInternalMessage(from, R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, conversationId).get(10000).asBytes();
        if(Arrays.equals(nonce, verifyNonce)) {
            parent.graduateToEstablishedConnection(from);
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_CONNECTION_ESTABLISHED, conversationId), from);
        }
    }
}