package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SL_PING;
import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SL_PONG;

/**
 * @author jokrey
 */
class PingProtocol {
    static void asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PING), to);
    }
    static void asAnswerer(P2LNodeInternal parent, SocketAddress from) throws IOException {
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PONG), from);
    }
}
