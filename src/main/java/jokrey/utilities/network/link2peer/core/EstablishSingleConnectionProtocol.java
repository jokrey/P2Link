package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.SecureRandom;
import java.util.Arrays;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class EstablishSingleConnectionProtocol {
    private static final SecureRandom secureRandom = new SecureRandom();

    public static void asReceiver(P2LNodeInternal parent, InetSocketAddress from, P2LMessage initialRequestMessage) throws IOException {
        if(parent.maxPeersReached()) {
            //potential exploit: a simple new connection, will cause this server to temporarily open a reverse connection to validate that the received link is actually valid
            //   this can be used to simultaneous and continuously have this node open new, reverse connections
            //   with this here can only be used for slow loris, though only slow loris with 4 bytes, soooo....
            parent.send(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST), from);
            System.out.println("refused connection request by "+from+" - max peers");
            return;
        }

//        if(!parent.getSelfLink().isPublicLinkKnown()) {
//            String ip = WhoAmIProtocol.whoAmI(parent, initialRequestMessage.sender, from);
//            System.out.println("ip = " + ip);
//            parent.attachIpToSelfLink(ip);
//        }

        P2Link peerLink = new P2Link(initialRequestMessage.asBytes());

        if(! peerLink.validateResolvesTo(from)) {
            parent.send(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST), from);
            System.out.println("refused connection request by "+from+" - does not resolve to same ip");
            return;
        }


        parent.addPotentialPeer(peerLink, from);

        // send a nonce to other peer
        //   if they are receiving correctly on their port they should read the nonce and be able to send it back
        //   (if they are not able to do this, then they may have spoofed their sender address and tried to fill this nodes peer list)
        byte[] nonce = new byte[16];
        secureRandom.nextBytes(nonce);
        parent.send(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST, nonce), from);

        byte[] verifyNonce = parent.futureForInternal(peerLink, R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER).get(2500).asBytes();
        if(Arrays.equals(nonce, verifyNonce))
            parent.graduateToActivePeer(peerLink);
        else
            parent.cancelPotentialPeer(peerLink);
    }

    public static void asRequester(P2LNodeInternal parent, P2Link link) throws IOException {
        SocketAddress outgoing = new InetSocketAddress(link.ipOrDns, link.port);
        parent.addPotentialPeer(link, outgoing);

        if(!parent.getSelfLink().isPublicLinkKnown()) {
            String ip = WhoAmIProtocol.whoAmI(parent, link, outgoing);
            System.out.println("ip = " + ip);
            parent.attachIpToSelfLink(ip);
        }

        parent.send(P2LMessage.createSendMessage(SL_PEER_CONNECTION_REQUEST, parent.getSelfLink().getRepresentingByteArray()), outgoing);

        byte[] verifyNonce = parent.futureForInternal(link, R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST).get(2500).asBytes();
        boolean connectionAccepted = verifyNonce.length == 16;
        if (connectionAccepted) {
            parent.send(P2LMessage.createSendMessage(R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER, verifyNonce), outgoing);
            parent.graduateToActivePeer(link);
        } else {
            parent.cancelPotentialPeer(link);
            throw new RequestRefusedException("Connection refused by peer");
        }
    }

    static class RequestRefusedException extends IOException {
        RequestRefusedException(String whyTho) {
            super(whyTho);
        }
    }
}
