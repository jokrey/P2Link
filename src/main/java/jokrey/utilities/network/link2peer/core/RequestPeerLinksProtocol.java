package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.network.link2peer.P2Link;
import java.io.IOException;
import java.util.Set;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class RequestPeerLinksProtocol {
//    public static P2Link[] requestPeersFrom(P2LNodeImpl node, P2Link link) throws IOException {
//        //without fully establishing connection
//        try (PeerConnection newCon = new PeerConnection.Outgoing(node, link)) {
//            return requestPeersFrom(newCon, false);
//        } //newCon closed here
//    }

    public static P2Link[] requestPeersFrom(PeerConnection connection) throws IOException {
        connection.sendSuperCause(SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS);

        LIbae libae = new LIbae(connection.futureRead(C_PEER_LINKS).get(3000).data);

        byte[][] rawPeers = libae.decodeAll();
        P2Link[] peers = new P2Link[rawPeers.length];
        for(int i=0;i<peers.length;i++)
            peers[i] = new P2Link(rawPeers[i]);

        return peers;
    }
    public static void answerRequest(PeerConnection connection, Set<P2Link> activePeerLinks) throws IOException {
        LIbae libae = new LIbae();
        for(P2Link activePeerLink : activePeerLinks)
            libae.encode(activePeerLink.getRepresentingByteArray());
        connection.send(C_PEER_LINKS, libae.getEncodedBytes());
    }
}
