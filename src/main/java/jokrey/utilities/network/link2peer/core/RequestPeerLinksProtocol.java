package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Set;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class RequestPeerLinksProtocol {
    public static P2Link[] requestPeersFrom(P2LNodeInternal parent, P2Link peer) throws IOException {
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        parent.send(P2LMessage.createSendMessage(SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS), peerConnection);

        P2LMessage peerLinksMessage = parent.futureForInternal(peer, C_PEER_LINKS).get(2500);

        ArrayList<P2Link> peers = new ArrayList<>();
        byte[] raw;
        while((raw = peerLinksMessage.nextVariable()) != null)
            peers.add(new P2Link(raw));
        return peers.toArray(new P2Link[0]);
    }
    public static void answerRequest(P2LNodeInternal parent, P2Link peer, Set<P2Link> activePeerLinks) throws IOException {
        SocketAddress peerConnection = parent.getActiveConnection(peer);

        int byteCounter = 0;
        byte[][] activePeerLinksRawified = new byte[activePeerLinks.size()*2][];
        int i = 0;
        for (P2Link active : activePeerLinks) {
            byte[] raw = active.getRepresentingByteArray();
            byte[] li = P2LMessage.makeVariableIndicatorFor(raw.length);
            activePeerLinksRawified[i++] = li;
            activePeerLinksRawified[i++] = raw;
            byteCounter+=li.length + raw.length;
        }

        parent.send(P2LMessage.createSendMessageWith(C_PEER_LINKS, byteCounter, activePeerLinksRawified), peerConnection);

        //differently optimized, though nicer looking:    - BOTH WORK
//        LIbae libae = new LIbae();
//        for(P2Link activePeerLink : activePeerLinks)
//            libae.encode(activePeerLink.getRepresentingByteArray());
//        parent.send(P2LMessage.createSendMessageFrom(C_PEER_LINKS, libae.getEncodedBytes()), peerConnection);
    }
}
