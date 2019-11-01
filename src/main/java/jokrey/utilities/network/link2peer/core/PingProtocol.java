package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SL_PONG;
import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SL_PING;

/**
 * @author jokrey
 */
public class PingProtocol {
    public static void asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PING, P2LMessage.EXPIRE_INSTANTLY), to);
    }
    static void asAnswerer(P2LNodeInternal parent, SocketAddress from) throws IOException {
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PONG, P2LMessage.EXPIRE_INSTANTLY), from);
    }
}
