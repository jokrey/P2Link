package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class EstablishSingleConnectionProtocol {
    public static void asReceiver(P2LNodeInternal parent, InetSocketAddress from, byte[] raw_packet_data, int data_length) throws IOException {

        if(parent.maxPeersReached()) {
            //potential exploit: a simple new connection, will cause this server to temporarily open a reverse connection to validate that the received link is actually valid
            //   this can be used to simultaneous and continuously have this node open new, reverse connections
            //   with this here can only be used for slow loris, though only slow loris with 4 bytes, soooo....
            parent.sendRaw(new P2LMessage(parent.getSelfLink(), R_CONNECTION_REQUEST_ANSWER, P2LMessage.fromBool(false)), from);
            return;
        }




        P2Link peerLink = new P2Link(new String(raw_packet_data, 4, data_length, StandardCharsets.UTF_8));

        if(! peerLink.validateResolvesTo(from)) {
            parent.sendRaw(new P2LMessage(parent.getSelfLink(), R_CONNECTION_REQUEST_ANSWER, P2LMessage.fromBool(false)), from);
            return;
        }

        //todo - only add active peer IF this method returns, i.e. it was possible to send a message back (this validates the link)
        parent.sendRaw(new P2LMessage(parent.getSelfLink(), R_CONNECTION_REQUEST_ANSWER, P2LMessage.fromBool(true)), from);

        parent.addActivePeer(peerLink, from);

//        parent.sendRaw(new P2LMessage(parent.getSelfLink(), R_IS_WHAT_I_THINK_MY_LINK_IS_WHAT_YOU_CONNECTED_TO, P2LMessage.fromBool(true)), from);
//        boolean ownLinkIsValid = parent.futureForInternal(peerLink, R_LINK_VALID).get(5000).asBool(); //mildly verifies that the connection is direct (i.e. not proxied) - would that be bad?
//        if(!ownLinkIsValid) {
//            return;
//        }

//        parent.sendRaw(new P2LMessage(parent.getSelfLink(), SL_LINK_VERIFY_REQUEST, parent.getSelfLink().getRepresentingByteArray()), from); //link verify still required with udp, because a
//        boolean peerLinkIsValid = peerLink.equals(new P2Link(parent.futureForInternal(peerLink, R_LINK_VALID).get(5000).data));
//
//        if(peerLinkIsValid)
//            parent.sendRaw(new P2LMessage(parent.getSelfLink(), R_LINK_VALID, new byte[0]), from);
    }

    public static void asRequester(P2LNodeInternal parent, P2Link link) throws IOException, RequestRefusedException {
        SocketAddress outgoing = new InetSocketAddress(link.ipOrDns, link.port);
        parent.sendRaw(new P2LMessage(parent.getSelfLink(), SL_PEER_CONNECTION_REQUEST, parent.getSelfLink().getRepresentingByteArray()), outgoing);

        parent.addActivePeer(link, outgoing);
//        parent.addLinkVerificationRequestPermission(link); //expect link validation on new connection handler
        boolean connectionAccepted = parent.futureForInternal(link, R_CONNECTION_REQUEST_ANSWER).get(5000).asBool();
        if(! connectionAccepted) {
            throw new RequestRefusedException("Connection refused by peer");
        }
//        P2Link remoteLinkSanityCheck = new P2Link(parent.futureForInternal(link, ));
//        if(! remoteLinkSanityCheck.equals(link)) {
//            throw new RequestRefusedException("Peer link was proxied");
//        }
//        rawConnection.send_int32(R_LINK_VALID);

//        try {
//            boolean success = parent.futureForInternal(link, R_LINK_VALID).get(5000).asBool();
//            if (!success) {
//                throw new RequestRefusedException("Link validation failed - remote could not establish reverse connection");
//            }
//        } catch(Throwable t) {
//            parent.revokeVerificationRequestPermission(link);
//            throw t;
//        }
//        boolean permissionStillOpen = parent.revokeVerificationRequestPermission(link);
//        if(permissionStillOpen) {
//            throw new IllegalStateException("permission open - i.e. peer said R_LINK_VALID and approved connection, but did not verify link, this indicates invalid behaviour and that the other peer does not use the default implementation");
//        }
    }

    public static class RequestRefusedException extends Exception {
        public RequestRefusedException(String whyTho) {
            super(whyTho);
        }
    }
}
