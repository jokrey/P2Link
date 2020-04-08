package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.bitsandbytes.BitHelper;
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

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.*;

/**
 * @author jokrey
 */
public class RelayedConnectionProtocol {
    private static final byte NAME_UNKNOWN = -1;
    private static final byte EXPECT_INCOMING_CONNECTION = 1;
    private static final byte CONTINUE_WITH_NAT_PUNCH = 2;
    private static final byte CONTINUE_WITH_NAT_PUNCH_MODIFY_DESTINATION_IP_TO_RELAY_IP = 3;

    public static boolean asInitiator(P2LNodeInternal parent, P2Link.Relayed to) {
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asInitiator - to = " + to);
        try {
            short conversationId = createConversationForRelay(parent);
            P2LConversation convo = parent.internalConvo(SL_CONNECTION_RELAY, conversationId, to.relayLink.resolve());
            convo.setA(100);

            if(parent.getSelfLink().isDirect()) {
                P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, conversationId);

                byte result = convo.initExpectClose(convo.encode(true, to.name)).nextByte();
                System.out.println(parent.getSelfLink()+" - d1 - result: "+result);
                if (result == EXPECT_INCOMING_CONNECTION) {
                    return reverseConnectionEstablishedFuture.get(5000);
                } else {
                    return false;
                }
            } else {
                P2LMessage m1 = convo.initExpect(convo.encode(false, to.name));
                byte result = m1.nextByte();
                System.out.println(parent.getSelfLink()+" - d2 - result: "+result);
                if (result == CONTINUE_WITH_NAT_PUNCH) {
                    P2Link.Direct directLinkOfRequestedName = (P2Link.Direct) P2Link.Direct.from(m1.nextVariable());

                    //before we close we will send a packet to the link, just so we create a hole in our own firewall.. We need no response, as it is too likely that it at least reached our router - creating the hole.
                    parent.sendInternalMessage(directLinkOfRequestedName.resolve(), P2LMessage.Factory.createNatHolePacket());

                    P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, conversationId);

                    convo.close(); //now we can close and the remote can attempt a connection to our ip (not known to us, but known to the peer we used to relay) - an ip which should now have an NAT-entry for the remote

                    return reverseConnectionEstablishedFuture.get(5000);
                } else if(result == CONTINUE_WITH_NAT_PUNCH_MODIFY_DESTINATION_IP_TO_RELAY_IP) {
                    P2Link.Direct directLinkOfRequestedName = new P2Link.Direct(to.relayLink.dnsOrIp, m1.nextInt());

                    //before we close we will send a packet to the link, just so we create a hole in our own firewall.. We need no response, as it is too likely that it at least reached our router - creating the hole.
                    parent.sendInternalMessage(directLinkOfRequestedName.resolve(), P2LMessage.Factory.createNatHolePacket());

                    P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, conversationId);

                    convo.close(); //now we can close and the remote can attempt a connection to our ip (not known to us, but known to the peer we used to relay) - an ip which should now have an NAT-entry for the remote

                    return reverseConnectionEstablishedFuture.get(5000);
                } else {
                    return false;
                }
            }
        } catch (TimeoutException | IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    //SL_REQUEST_DIRECT_CONNECT_TO
    public static void asAnswerer_ConnectTo(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asAnswerer_ConnectTo");
        convo.close();
        P2Link.Direct toConnectTo = (P2Link.Direct) P2Link.Direct.from(m0.asBytes());
        System.out.println(parent.getSelfLink()+" - toConnectTo = " + toConnectTo);
        DirectConnectionProtocol.asInitiator(parent, toConnectTo.resolve(), convo.getConversationId());
    }

    //SL_REQUEST_DIRECT_CONNECT_TO_MODIFY_DESTINATION_IP_TO_RELAY_IP
    public static void asAnswerer_ConnectTo_modifyDestinationIpToRelayIp(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asAnswerer_ConnectTo_modifyDestinationIpToRelayIp");
        convo.close();
        P2Link.Direct toConnectTo = new P2Link.Direct(convo.getPeer().getHostName(), m0.nextInt());
        System.out.println(parent.getSelfLink()+" - toConnectTo = " + toConnectTo);
        DirectConnectionProtocol.asInitiator(parent, toConnectTo.resolve(), convo.getConversationId());
    }

    //SL_CONNECTION_RELAY
    public static void asAnswerer_RelayConnection(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        convo.setA(100);
        InetSocketAddress rawAddressRequesterPeer = convo.getPeer();
        System.out.println(parent.getSelfLink()+" - RelayedConnectionProtocol.asAnswerer_RelayConnection - "+convo.getPeer());
        int connectionId = convo.getConversationId();
        boolean isRequestComingFromPubliclyAvailableLink = m0.nextBool();
        String nameOfSecondPeer = m0.nextVariableString();

        InetSocketAddress rawAddressSecondPeer = parent.resolveByName(nameOfSecondPeer);
        if(rawAddressSecondPeer == null) {
            convo.closeWith(convo.encode(NAME_UNKNOWN));
            return;
        }
        byte[] directLinkToSecondPeerBytes = new P2Link.Direct(rawAddressSecondPeer).toBytes();
        byte[] directLinkToRequesterPeerBytes = new P2Link.Direct(rawAddressRequesterPeer).toBytes();

        //we do not inform the peer that requested the relay of the result, nor does the second peer inform us whether it accepted the request.
        //  the initially requesting peer will notice via timeout that no one connected to it.
        if(isRequestComingFromPubliclyAvailableLink) {
            System.out.println(parent.getSelfLink()+" - isRequestComingFromPubliclyAvailableLink");

            //No need to NAT punch, requester is public
            convo.closeWith(convo.encode(EXPECT_INCOMING_CONNECTION));
            P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
            convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
        } else {

            boolean isRequesterPeerInLocalSubnet = NetUtil.isV4AndFromSameSubnet(rawAddressRequesterPeer.getAddress(), parent.getLocalIPv4InterfaceAddress());
            boolean isSecondPeerInLocalSubnet = NetUtil.isV4AndFromSameSubnet(rawAddressSecondPeer.getAddress(), parent.getLocalIPv4InterfaceAddress());

            if(isRequesterPeerInLocalSubnet == isSecondPeerInLocalSubnet) { //i.e. either both in WAN or both in LAN - i.e. nat punch is either not required but works anyways or is required and hopefully works(nat config)
                System.out.println(parent.getSelfLink()+" - isRequesterPeerInLocalSubnet == isSecondPeerInLocalSubnet");
                //NAT PUNCH - requester has already send punching packet(which will likely not be received by remote) - so now the remote can start sending
                convo.answerClose(convo.encode(CONTINUE_WITH_NAT_PUNCH, directLinkToSecondPeerBytes)); //THIS HAS TO COMPLETE BEFORE WE TELL SECOND PEER TO CONNECT - IT CONTAINS THE NAT PUNCH INITIAL MESSAGE

                //now that a nat hole exists in the requester's nat, we can simply initiate a connection to it from the second peer (which will create a nat hole with its first message)
                P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
                convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
            } else if(isSecondPeerInLocalSubnet) { //requires the server to be behind a symmetric nat
                System.out.println(parent.getSelfLink()+" - isSecondPeerInLocalSubnet");

                //We still do the NAT punch, but we assume that our nat is SYMMETRIC and that it's outgoing port will be the same that we see here...
                //   however since it is in the same subnet we are, we let the remote modify the ip to ours - hoping that we and the second peer are behind the same nat
                convo.answerClose(convo.encode(CONTINUE_WITH_NAT_PUNCH_MODIFY_DESTINATION_IP_TO_RELAY_IP, rawAddressSecondPeer.getPort())); //THIS HAS TO COMPLETE BEFORE WE TELL SECOND PEER TO CONNECT - IT CONTAINS THE NAT PUNCH INITIAL MESSAGE

                //continue normally
                P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
                convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
            } else if(isRequesterPeerInLocalSubnet) { //requires the server to be behind a symmetric nat
                System.out.println(parent.getSelfLink()+" - isRequesterPeerInLocalSubnet");
                //the requester can do a normal nat punch
                convo.answerClose(convo.encode(CONTINUE_WITH_NAT_PUNCH, directLinkToSecondPeerBytes)); //THIS HAS TO COMPLETE BEFORE WE TELL SECOND PEER TO CONNECT - IT CONTAINS THE NAT PUNCH INITIAL MESSAGE

                //but the second peer will have to connect to a modified address, consisting of what it sees as source ip from our request and the port of the requester peer
                P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO_MODIFY_DESTINATION_IP_TO_RELAY_IP, connectionId, rawAddressSecondPeer);
                convoWithSecondPeer.initClose(convoWithSecondPeer.encode(rawAddressRequesterPeer.getPort()));
            }

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
