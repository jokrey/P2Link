package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.NetUtil;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.net.*;
import java.util.function.BiConsumer;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_CONNECTION_RELAY;
import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.SL_REQUEST_DIRECT_CONNECT_TO;

/**
 * @author jokrey
 */
public class RelayedConnectionProtocol {
    private static final byte NAME_UNKNOWN = -1;
    private static final byte SUCCESS = 1;

    public static boolean asInitiator(P2LNodeInternal parent, P2Link.Relayed to) {
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asInitiator - to = " + to);
        try {
            short conversationId = createConversationForRelay(parent);
            P2LConversation convo = parent.internalConvo(SL_CONNECTION_RELAY, conversationId, to.relayLink.resolve());

            if(parent.getSelfLink().isDirect()) {
                P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, conversationId);

                byte result = convo.initExpectClose(convo.encode(true, to.name)).nextByte();
                System.out.println(parent.getSelfLink()+" - d1 - result: "+result);
                if(result == NAME_UNKNOWN || result != SUCCESS) {
                    return false;
                } else {
                    return reverseConnectionEstablishedFuture.get(5000);
                }
            } else {
                P2LMessage m1 = convo.initExpect(convo.encode(false, to.name));
                byte result = m1.nextByte();
                System.out.println(parent.getSelfLink()+" - d2 - result: "+result);
                if(result == NAME_UNKNOWN || result != SUCCESS) {
                    return false;
                } else {
                    P2Link.Direct directLinkOfRequestedNameAsSeenFromRelayLinkPeer = (P2Link.Direct) P2Link.Direct.from(m1.nextVariable());

                    //before we close we will send a packet to the link, just so we create a hole in our own firewall.. We need no response, as it is too likely that it at least reached our router - creating the hole.
                    parent.sendInternalMessage(directLinkOfRequestedNameAsSeenFromRelayLinkPeer.resolve(), P2LMessage.Factory.createNatHolePacket());

                    P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, conversationId);

                    convo.close(); //now we can close and the remote can attempt a connection to our ip (not known to us, but known to the peer we used to relay) - an ip which should now have an NAT-entry for the remote

                    return reverseConnectionEstablishedFuture.get(5000);
                }
            }
        } catch (TimeoutException | IOException e) {
            return false;
        }
    }

    public static void asAnswerer_ConnectTo(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asAnswerer_ConnectTo");
        convo.close();
        P2Link.Direct toConnectTo = (P2Link.Direct) P2Link.Direct.from(m0.asBytes());
        System.out.println(parent.getSelfLink()+" - toConnectTo = " + toConnectTo);
        DirectConnectionProtocol.asInitiator(parent, toConnectTo.resolve(), convo.getConversationId());
    }

    //SL_CONNECTION_RELAY
    public static void asAnswerer_RelayConnection(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asAnswerer_RelayConnection");
        int connectionId = convo.getConversationId();
        boolean isRequestComingFromPubliclyAvailableLink = m0.nextBool();
        String nameOfSecondPeer = m0.nextVariableString();

        InetSocketAddress rawAddressSecondPeer = parent.resolveByName(nameOfSecondPeer);
        if(rawAddressSecondPeer == null) {
            convo.closeWith(convo.encode(NAME_UNKNOWN));
            return;
        }
        byte[] directLinkToSecondPeerBytes = new P2Link.Direct(rawAddressSecondPeer).toBytes();
        byte[] directLinkToRequesterPeerBytes = new P2Link.Direct(convo.getPeer()).toBytes();

        //we do not inform the peer that requested the relay of the result, nor does the second peer inform us whether it accepted the request.
        //  the initially requesting peer will notice via timeout that no one connected to it.
        if(isRequestComingFromPubliclyAvailableLink) {
            //No need to NAT punch, requester is public
            convo.closeWith(convo.encode(SUCCESS));
            P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
            convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
        } else {

            InterfaceAddress localIPv4InterfaceAddress = NetUtil.getLocalIPv4InterfaceAddress();
            boolean isRequesterPeerInLocalSubnet = NetUtil.isV4AndFromSameSubnet(rawAddressSecondPeer.getAddress(), localIPv4InterfaceAddress);
            boolean isSecondPeerInLocalSubnet = NetUtil.isV4AndFromSameSubnet(rawAddressSecondPeer.getAddress(), localIPv4InterfaceAddress);

            //NAT PUNCH - requester has already send punching packet(which will likely not be received by remote) - so now the remote can start sending
            convo.answerClose(convo.encode(SUCCESS, directLinkToSecondPeerBytes));

            P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
            convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
        }
    }






    private static P2LFuture<Boolean> waitForReverseConnection(P2LNodeInternal parent, short requiredConversationId) {
        P2LFuture<Boolean> future = new P2LFuture<>();
        BiConsumer<InetSocketAddress, Integer> listener = (link, connectConversationId) -> {
            System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.waitForReverseConnection SUCCESS:");
            System.out.println(parent.getSelfLink()+" - conversationId = " + requiredConversationId);
            System.out.println(parent.getSelfLink()+" - connectConversationId = " + connectConversationId);
            if(requiredConversationId == connectConversationId)
                future.trySetCompleted(true);
        };
        parent.addConnectionEstablishedListener(listener);
        future.callMeBack(b -> {
            System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.waitForReverseConnection FIN WITH CANCEL");
            parent.removeConnectionEstablishedListener(listener);
        });

        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.waitForReverseConnection WAITING");

        return future;
    }

    //GUARANTEED DIFFERENT CONVERSATION IDS FOR INITIAL DIRECT AND REVERSE CONNECTIONS, BECAUSE OF:
    // theoretical problem: coincidentally it could be possible that a peer initiates a connection using the same conversationId (SOLUTION: NEGATIVE AND POSITIVE CONVERSATION IDS)
    public static short createConversationForInitialDirect(P2LNodeInternal parent) {
        return (short) -abs(parent.createUniqueConversationId()); //ALWAYS NEGATIVE
    }
    public static short createConversationForRelay(P2LNodeInternal parent) {
        short conversationId;
        do {
            conversationId = parent.createUniqueConversationId();
        } while(conversationId==Short.MIN_VALUE); //abs(min value) == min value, therefore min value illegal here
        return abs(conversationId); //ALWAYS POSITIVE
    }
    public static short abs(short a) {
        return (a < 0) ? (short) -a : a;
    }
}
