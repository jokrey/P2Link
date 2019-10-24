package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class RequestPeerLinksProtocol {
//    public static P2Link[] requestPeersFrom(P2LNodeImpl node, P2Link link) throws IOException {
//        //without fully establishing connection
//        try (PeerConnection newCon = new PeerConnection.Outgoing(node, link)) {
//            return requestPeersFrom(newCon, false);
//        } //newCon closed here
//    }

    public static P2Link[] requestPeersFrom(P2LNodeInternal parent, P2Link peer) throws IOException {
//        System.out.println("RequestPeerLinksProtocol.requestPeersFrom");
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        parent.sendRaw(new P2LMessage(parent.getSelfLink(), SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS, new byte[0]), peerConnection);

        LIbae libae = new LIbae(parent.futureForInternal(peer, C_PEER_LINKS).get(5000).data);

        byte[][] rawPeers = libae.decodeAll();
//        System.out.println("rawPeers.length = " + rawPeers.length);
        P2Link[] peers = new P2Link[rawPeers.length];
        for(int i=0;i<peers.length;i++) {
//            System.out.println("rawPeers[i] = " + new String(rawPeers[i], StandardCharsets.UTF_8));
            peers[i] = new P2Link(rawPeers[i]);
        }

        return peers;
    }
    public static void answerRequest(P2LNodeInternal parent, P2Link peer, Set<P2Link> activePeerLinks) throws IOException {
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        LIbae libae = new LIbae();
        for(P2Link activePeerLink : activePeerLinks)
            libae.encode(activePeerLink.getRepresentingByteArray());
        parent.sendRaw(new P2LMessage(parent.getSelfLink(), C_PEER_LINKS, libae.getEncodedBytes()), peerConnection);
    }
}
