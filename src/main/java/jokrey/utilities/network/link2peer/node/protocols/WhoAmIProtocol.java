package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;

import java.io.IOException;
import java.net.InetSocketAddress;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_WHO_AM_I;

/**
 * @author jokrey
 */
public class WhoAmIProtocol {
    public static P2Link.Direct asInitiator(P2LNodeInternal parent, InetSocketAddress to) throws IOException {
        P2LConversation convo = parent.internalConvo(SL_WHO_AM_I, to);
        return (P2Link.Direct) P2Link.from(convo.initExpectClose());
//        return P2Link.from(parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
//                P2LFuture.before(() ->
//                                parent.sendInternalMessage(to, P2LMessage.Factory.createSendMessageWith(SL_WHO_AM_I)),
//                        parent.expectInternalMessage(to, R_WHO_AM_I_ANSWER))
//        ).asBytes());
    }
    public static void asAnswerer(P2LConversation convo) throws IOException {
        InetSocketAddress address = convo.getPeer();
        convo.closeWith(new P2Link.Direct(address.getAddress().getCanonicalHostName(), address.getPort()).toBytes());
    }



//    public static P2Link asInitiator(P2LNodeInternal parent, InetSocketAddress to) throws IOException {
//        return P2Link.from(parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
//                P2LFuture.before(() ->
//                                parent.sendInternalMessage(to, P2LMessage.Factory.createSendMessageWith(SL_WHO_AM_I)),
//                        parent.expectInternalMessage(to, R_WHO_AM_I_ANSWER))
//        ).asBytes());
//    }
//    public static void asAnswerer(P2LNodeInternal parent, DatagramPacket receivedPacket) throws IOException {
//        parent.sendInternalMessage(receivedPacket.getSocketAddress(), P2LMessage.Factory.createSendMessage(R_WHO_AM_I_ANSWER,
//                P2Link.createPublicLink(receivedPacket.getAddress().getCanonicalHostName(), receivedPacket.getPort()).getBytesRepresentation()
//        ));
//    }
}
