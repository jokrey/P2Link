package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import java.io.IOException;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * @author jokrey
 */
public class PingProtocol {
    public static P2LFuture<SocketAddress> asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        byte[] nonce = new byte[16];
        EstablishSingleConnectionProtocol.secureRandom.nextBytes(nonce);
        P2LFuture<SocketAddress> expectation = parent.expectInternalMessage(to, R_PONG).toType(pong -> {
            if(!pong.payloadEquals(nonce))
                return null;//cancels future
            return to;
        });
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(SL_PING, nonce), to);
        return expectation;
    }
    static void asAnswerer(P2LNodeInternal parent, SocketAddress from, P2LMessage initialMessage) throws IOException {
        parent.sendInternalMessage(P2LMessage.Factory.createSendMessage(R_PONG, P2LMessage.EXPIRE_INSTANTLY, initialMessage.asBytes()), from);
    }
}
