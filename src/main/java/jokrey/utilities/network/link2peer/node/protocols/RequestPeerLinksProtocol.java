package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.encoder.as_union.li.bytes.LIbae;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS;

public class RequestPeerLinksProtocol {
    public static P2LFuture<List<P2Link>> asInitiator(P2LNodeInternal parent, InetSocketAddress to) {
//        return P2LThreadPool.executeSingle(() -> {
//            P2LConversation convo = parent.internalConvo(SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS, to);
//            convo.setRM(300);
//            P2LMessage message = convo.initExpectClose(new byte[0]);
//
//            ArrayList<P2Link> peers = new ArrayList<>();
//            String raw;
//            while((raw = message.nextVariableString()) != null) {
//                P2Link link = P2Link.from(raw);
//                if(link.isOnlyLocal())
//                    peers.add(((P2Link.Local) link).withRelay(new P2Link.Direct(to)));
//                else
//                    peers.add(link);
//            }
//            return peers;
//        });



        P2LConversation convo = parent.internalConvo(SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS, to);
        convo.setRM(300);
        return convo.initExpectCloseAsync(convo.encoder()).toType(m1 -> {
            System.out.println("m1 = " + m1);
            ArrayList<P2Link> peers = new ArrayList<>();
            String raw;
            while((raw = m1.nextVariableString()) != null) {
                P2Link link = P2Link.from(raw);
                if(link.isOnlyLocal())
                    peers.add(((P2Link.Local) link).withRelay(new P2Link.Direct(to)));
                else
                    peers.add(link);
            }
            return peers;
        });
    }
    public static void asAnswerer(P2LNodeInternal parent, P2LConversation convo) throws IOException {
        P2LConnection[] origEstablishedConnections = parent.getEstablishedConnections();
        LIbae lIbae = new LIbae();
        for (P2LConnection origEstablishedConnection : origEstablishedConnections)
            if (!origEstablishedConnection.address.equals(convo.getPeer()))
                lIbae.encode(origEstablishedConnection.link.toBytes());
        convo.closeWith(lIbae.getEncodedBytes());
    }
}
