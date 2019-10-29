package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.C_PEER_LINKS;
import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS;

class RequestPeerLinksProtocol {
    static String[] asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        return parent.tryReceive(3, 500, () -> {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS), to);
            return parent.expectInternalMessage(to, C_PEER_LINKS).toType(message -> {
                ArrayList<String> peers = new ArrayList<>();
                String raw;
                while((raw = message.nextVariableString()) != null)
                    peers.add(raw);
                return peers.toArray(new String[0]);
            });
        });
    }
    static void asAnswerer(P2LNodeInternal parent, SocketAddress from) throws IOException {
        SocketAddress[] origEstablishedConnections = parent.getEstablishedConnections().toArray(new SocketAddress[0]);
        ArrayList<String> establishedAsStrings = new ArrayList<>(origEstablishedConnections.length);
        for (SocketAddress origEstablishedConnection : origEstablishedConnections)
            if (!origEstablishedConnection.equals(from))
                establishedAsStrings.add(WhoAmIProtocol.toString(origEstablishedConnection));

        parent.sendInternalMessage(P2LMessage.Factory.createSendMessageFromVariables(C_PEER_LINKS, establishedAsStrings), from);
    }
}
