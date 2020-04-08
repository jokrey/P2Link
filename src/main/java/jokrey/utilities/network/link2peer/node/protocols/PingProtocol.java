package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_PING;

/**
 * @author jokrey
 */
public class PingProtocol {
    public static P2LFuture<Boolean> asInitiator(P2LNodeInternal parent, InetSocketAddress to) throws IOException {
        return parent.sendInternalMessageWithReceipt(to, P2LMessage.with(SL_PING)); //no need for a conversation id... if we receive a pong from a previous attempt we are happy too.
    }
//    static void asAnswerer(P2LNodeInternal parent, InetSocketAddress from) throws IOException {
//        answering done automatically through receipts
//    }
}
