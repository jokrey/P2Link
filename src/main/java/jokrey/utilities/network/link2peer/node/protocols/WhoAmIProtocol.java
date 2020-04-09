package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import java.io.IOException;
import java.net.InetSocketAddress;
import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_WHO_AM_I;

/**
 * @author jokrey
 */
public class WhoAmIProtocol {
    public static P2LFuture<P2Link.Direct> asInitiator(P2LNodeInternal parent, InetSocketAddress to) {
        P2LConversation convo = parent.internalConvo(SL_WHO_AM_I, to);
        return convo.initExpectCloseAsync(convo.encoder()).toType(m1 -> (P2Link.Direct) P2Link.from(m1.asBytes()));
    }
    public static void asAnswerer(P2LConversation convo) throws IOException {
        InetSocketAddress address = convo.getPeer();
        convo.closeWith(new P2Link.Direct(address.getAddress().getCanonicalHostName(), address.getPort()).toBytes());
    }
}
