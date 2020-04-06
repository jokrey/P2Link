package jokrey.utilities.network.link2peer.node.protocols_old;

public class RequestPeerLinksProtocolOld {
//    public static int C_PEER_LINKS = -1111;
//    public static List<P2Link> asInitiator(P2LNodeInternal parent, InetSocketAddress to) throws IOException {
//        return parent.tryReceive(P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_COUNT, P2LHeuristics.DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT, () ->
//                P2LFuture.before(() ->
//                        parent.sendInternalMessage(to, P2LMessage.Factory.createSendMessage(SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS)),
//                        parent.expectInternalMessage(to, C_PEER_LINKS))
//                .toType(message -> {
//                    ArrayList<P2Link> peers = new ArrayList<>();
//                    String raw;
//                    while((raw = message.nextVariableString()) != null)
//                        peers.add(P2Link.fromEnsureRelayLinkAvailable(raw, (InetSocketAddress) to));
//                    return peers;
//                }));
//    }
//    public static void asAnswerer(P2LNodeInternal parent, InetSocketAddress fromRaw) throws IOException {
//        InetSocketAddress[] origEstablishedConnections = parent.getEstablishedConnections().toArray(new InetSocketAddress[0]);
//        ArrayList<String> establishedAsStrings = new ArrayList<>(origEstablishedConnections.length);
//        for (InetSocketAddress origEstablishedConnection : origEstablishedConnections)
//            if (!origEstablishedConnection.equals(fromRaw))
//                establishedAsStrings.add(origEstablishedConnection.toString());
//
//        parent.sendInternalMessage(fromRaw, P2LMessage.Factory.createSendMessageFromVariables(C_PEER_LINKS, establishedAsStrings));
//    }
}
