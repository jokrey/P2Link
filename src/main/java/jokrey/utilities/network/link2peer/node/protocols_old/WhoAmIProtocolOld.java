package jokrey.utilities.network.link2peer.node.protocols_old;

/**
 * @author jokrey
 */
public class WhoAmIProtocolOld {
//    static int R_WHO_AM_I_ANSWER = -999;
//    public static P2Link asInitiator(P2LNodeInternal parent, InetSocketAddress to) throws IOException {
//        return P2Link.from(parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
//                P2LFuture.before(() ->
//                                parent.sendInternalMessage(to, P2LMessage.createSendMessageWith(SL_WHO_AM_I)),
//                        parent.expectInternalMessage(to, R_WHO_AM_I_ANSWER))
//        ).asBytes());
//    }
//    public static void asAnswerer(P2LNodeInternal parent, DatagramPacket receivedPacket) throws IOException {
//        parent.sendInternalMessage((InetSocketAddress) receivedPacket.getSocketAddress(), P2LMessage.Factory.createSendMessage(R_WHO_AM_I_ANSWER,
//                new P2Link.Direct(receivedPacket.getAddress().getCanonicalHostName(), receivedPacket.getPort()).toBytes()
//        ));
//    }
}
