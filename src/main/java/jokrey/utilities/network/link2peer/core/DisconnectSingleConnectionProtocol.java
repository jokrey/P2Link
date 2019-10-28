package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

class DisconnectSingleConnectionProtocol {
    public static void asInitiator(P2LNodeInternal parent, SocketAddress to) {
        parent.markBrokenConnection(to, false);
        try {
            parent.sendInternalMessage(P2LMessage.createSendMessage(SC_DISCONNECT), to);
        } catch (IOException ignored) { }
    }

    public static void asReceiver(P2LNodeInternal parent, SocketAddress from) {
        parent.markBrokenConnection(from, false);
    }
}
