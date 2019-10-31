package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.SC_DISCONNECT;

class DisconnectSingleConnectionProtocol {
    static void asInitiator(P2LNodeInternal parent, SocketAddress to) {
        parent.markBrokenConnection(to, false);
        try {
            parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SC_DISCONNECT), to);
        } catch (IOException ignored) { }
    }

    static void asAnswerer(P2LNodeInternal parent, SocketAddress from) {
        parent.markBrokenConnection(from, false);
    }
}
