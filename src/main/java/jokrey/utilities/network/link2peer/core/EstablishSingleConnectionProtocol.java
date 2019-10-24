package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.mcnp.MCNP_Connection;

import java.io.IOException;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class EstablishSingleConnectionProtocol {
    public static PeerConnection asReceiver(P2LNodeInternal parent, MCNP_Connection rawConnection) throws IOException {
        if(parent.maxPeersReached()) {
            //potential exploit: a simple new connection, will cause this server to temporarily open a reverse connection to validate that the received link is actually valid
            //   this can be used to simultaneous and continuously have this node open new, reverse connections
            //   with this here can only be used for slow loris, though only slow loris with 4 bytes, soooo....
            rawConnection.send_int32(R_CONNECTION_DENIED_TOO_MANY_PEERS);
            return null;
        } else {
            rawConnection.send_int32(R_CONNECTION_ACCEPTED);
        }

        rawConnection.send_utf8(parent.getSelfLink().raw);
        rawConnection.flush();
        boolean ownLinkIsValid = rawConnection.receive_int32() == R_LINK_VALID; //mildly verifies that the connection is direct (i.e. not proxied) - would that be bad?
        if(!ownLinkIsValid) {
            return null;
        }

        P2Link peerLink = new P2Link(rawConnection.receive_utf8());
        PeerConnection.Outgoing reverseLinkValidationConnection = new PeerConnection.Outgoing(parent, peerLink);
        reverseLinkValidationConnection.raw().send_cause(IC_CONNECTION_REQUEST_LINK_VERIFY);
        reverseLinkValidationConnection.raw().send_utf8(parent.getSelfLink().raw);
        boolean peerLinkIsValid = peerLink.equals(new P2Link(reverseLinkValidationConnection.raw().receive_utf8()));
        reverseLinkValidationConnection.tryClose();

        if(peerLinkIsValid) {
            PeerConnection peerConnection = new PeerConnection.Incoming(parent, peerLink, rawConnection);
            rawConnection.send_int32(R_LINK_VALID);
            return peerConnection;
        } else {
            return null;
        }
    }

    public static PeerConnection asRequester(P2LNodeInternal parent, P2Link link) throws IOException, RequestRefusedException {
        PeerConnection connection = new PeerConnection.Outgoing(parent, link);

        MCNP_Connection rawConnection = connection.raw();

        rawConnection.send_cause(IC_NEW_PEER_CONNECTION);
        rawConnection.flush();

        boolean connectionAccepted = rawConnection.receive_int32() == R_CONNECTION_ACCEPTED;
        if(! connectionAccepted) {
            connection.tryClose();
            throw new RequestRefusedException("Connection refused by peer");
        }
        P2Link remoteLinkSanityCheck = new P2Link(rawConnection.receive_utf8());
        if(! remoteLinkSanityCheck.equals(link)) {
            connection.tryClose();
            throw new RequestRefusedException("Peer link was proxied");
        }
        rawConnection.send_int32(R_LINK_VALID);

        try {
            parent.addLinkVerificationRequestPermission(link);
            rawConnection.send_utf8(parent.getSelfLink().raw);
            rawConnection.flush();
            //expect link validation on new connection handler

            boolean success = rawConnection.receive_int32() == R_LINK_VALID;
            if (!success) {
                connection.tryClose();
                throw new RequestRefusedException("Link validation failed - remote could not establish reverse connection");
            }
        } catch(Throwable t) {
            parent.revokeVerificationRequestPermission(link);
            throw t;
        } finally {
            boolean permissionStillOpen = parent.revokeVerificationRequestPermission(link);
            if(permissionStillOpen) {
                rawConnection.tryClose();
                throw new IllegalStateException("permission open - i.e. peer said R_LINK_VALID and approved connection, but did not verify link, this indicates invalid behaviour and that the other peer does not use the default implementation");
            }
        }

        return connection;
    }

    public static class RequestRefusedException extends Exception {
        public RequestRefusedException(String whyTho) {
            super(whyTho);
        }
    }
}
