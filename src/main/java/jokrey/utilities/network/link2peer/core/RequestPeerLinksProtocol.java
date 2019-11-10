package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.C_PEER_LINKS;
import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS;

class RequestPeerLinksProtocol {
    static List<P2Link> asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        return parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
                parent.expectInternalMessage(to, C_PEER_LINKS)
                .nowOrCancel(() -> parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS), to))
                .toType(message -> {
                    ArrayList<P2Link> peers = new ArrayList<>();
                    String raw;
                    while((raw = message.nextVariableString()) != null)
                        peers.add(P2Link.fromString(raw));
                    return peers;
                }));
    }
    static void asAnswerer(P2LNodeInternal parent, SocketAddress fromRaw) throws IOException {
        P2Link[] origEstablishedConnections = parent.getEstablishedConnections().toArray(new P2Link[0]);
        ArrayList<String> establishedAsStrings = new ArrayList<>(origEstablishedConnections.length);
        for (P2Link origEstablishedConnection : origEstablishedConnections)
            if (!origEstablishedConnection.getSocketAddress().equals(fromRaw))
                establishedAsStrings.add(origEstablishedConnection.getStringRepresentation());

        parent.sendInternalMessage(P2LMessage.Factory.createSendMessageFromVariables(C_PEER_LINKS, P2LMessage.EXPIRE_INSTANTLY, establishedAsStrings), fromRaw);
    }
}
