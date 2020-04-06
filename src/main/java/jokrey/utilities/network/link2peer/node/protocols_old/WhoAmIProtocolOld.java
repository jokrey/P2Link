package jokrey.utilities.network.link2peer.node.protocols_old;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.P2LHeuristics;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_WHO_AM_I;

/**
 * @author jokrey
 */
public class WhoAmIProtocolOld {
    static int R_WHO_AM_I_ANSWER = -999;
    public static P2Link asInitiator(P2LNodeInternal parent, SocketAddress to) throws IOException {
        return P2Link.fromBytes(parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
                P2LFuture.before(() ->
                                parent.sendInternalMessage(to, P2LMessage.Factory.createSendMessageWith(SL_WHO_AM_I)),
                        parent.expectInternalMessage(to, R_WHO_AM_I_ANSWER))
        ).asBytes());
    }
    public static void asAnswerer(P2LNodeInternal parent, DatagramPacket receivedPacket) throws IOException {
        parent.sendInternalMessage(receivedPacket.getSocketAddress(), P2LMessage.Factory.createSendMessage(R_WHO_AM_I_ANSWER,
                P2Link.createDirectLink(receivedPacket.getAddress().getCanonicalHostName(), receivedPacket.getPort()).getBytesRepresentation()
        ));
    }
}
