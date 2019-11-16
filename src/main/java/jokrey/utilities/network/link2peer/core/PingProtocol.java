package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2LInternalMessageTypes.SL_PING;

/**
 * @author jokrey
 */
class PingProtocol {
    static P2LFuture<Boolean> asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        return parent.sendInternalMessageWithReceipt(P2LMessage.Factory.createSendMessage(SL_PING, parent.createUniqueConversationId()), to);
    }
//    static void asAnswerer(P2LNodeInternal parent, SocketAddress from) throws IOException {
//        answering done automatically through receipts
//    }
}
