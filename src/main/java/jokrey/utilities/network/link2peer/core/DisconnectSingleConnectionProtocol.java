package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;

import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SC_DISCONNECT;

class DisconnectSingleConnectionProtocol {
    static void asInitiator(P2LNodeInternal parent, P2Link to) {
        parent.markBrokenConnection(to, false);
        try {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SC_DISCONNECT), to.getSocketAddress()); //if this message fails, the other node will ping and it will automatically disconnect then ( this is just a courtesy)
        } catch (IOException ignored) { }
    }

    static void asAnswerer(P2LNodeInternal parent, SocketAddress from) {
        P2Link established = parent.toEstablished(from);
        if(established!=null)
            parent.markBrokenConnection(established, false);
    }
}
