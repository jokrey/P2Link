package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;

import java.io.IOException;
import java.net.InetSocketAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SC_DISCONNECT;

public class DisconnectSingleConnectionProtocol {
    public static void asInitiator(P2LNodeInternal parent, InetSocketAddress to) {
        parent.markBrokenConnection(to, false);
        try {
            parent.sendInternalMessage(to, P2LMessage.with(SC_DISCONNECT)); //if this message fails, the other node will ping and it will automatically disconnect then ( this is just a courtesy)
        } catch (IOException ignored) { }
    }

    public static void asAnswerer(P2LNodeInternal parent, InetSocketAddress from) {
        parent.markBrokenConnection(from, false);
    }
}
