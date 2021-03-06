package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.conversation.P2LConversation;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.NetUtil;
import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.util.function.BiConsumer;

import static jokrey.utilities.network.link2peer.node.core.P2LInternalMessageTypes.*;

/**
 * TODO
 *   Note: There is a single logical scenario that still creates issues.
 *       (Though it is fringe(app depended), should kinda work[router config thing] and maybe not solvable programmatically)
 *       If two computers in the same subnet as the relay server, attempt to connect over the relay server's public ip (if one of them does it, that is enough):
 *          then the nat punch would go over the router's gateway address which is somehow not possible
 *
 * @author jokrey
 */
public class RelayedConnectionProtocol {
    private static final byte NAME_UNKNOWN = -1;
    private static final byte EXPECT_INCOMING_CONNECTION = 1;
    private static final byte CONTINUE_WITH_NAT_PUNCH = 2;
    private static final byte CONTINUE_WITH_NAT_PUNCH_MODIFY_DESTINATION_IP_TO_RELAY_IP = 3;

    public static P2LFuture<Boolean> asInitiator(P2LNodeInternal parent, P2Link.Relayed to) {
        short conversationId = createConversationForRelay(parent);
        P2LConversation convo = parent.internalConvo(SL_CONNECTION_RELAY, conversationId, to.relayLink.resolve());
        convo.setRM(300); //for some reason the first packet takes a while to complete - so lets wait a little longer

        if(parent.getSelfLink().isDirect()) {
            P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, to, conversationId);//should be registered before..

            return convo.initExpectCloseAsync(convo.encode(true, to.name)).andThen(m1 -> {
                byte result = m1.nextByte();
                if (result == EXPECT_INCOMING_CONNECTION) {
                    return reverseConnectionEstablishedFuture;
                } else {
                    return new P2LFuture<>(false);
                }
            });
        } else {
            return convo.initExpectAsync(convo.encode(false, to.name)).andThen(m1 -> {
                try {
                    byte result = m1.nextByte();
                    if (result == CONTINUE_WITH_NAT_PUNCH) {
                        P2Link.Direct directLinkOfRequestedName = (P2Link.Direct) P2Link.Direct.from(m1.nextVariable());

                        System.out.println("directLinkOfRequestedName = " + directLinkOfRequestedName);

                        //before we close we will send a packet to the link, just so we create a hole in our own firewall.. We need no response, as it is too likely that it at least reached our router - creating the hole.
                        parent.sendInternalMessage(directLinkOfRequestedName.resolve(), P2LMessage.createNatHolePacket());

                        P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, to, conversationId);

                        convo.close(); //now we can close and the remote can attempt a connection to our ip (not known to us, but known to the peer we used to relay) - an ip which should now have an NAT-entry for the remote

                        return reverseConnectionEstablishedFuture;
                    } else if (result == CONTINUE_WITH_NAT_PUNCH_MODIFY_DESTINATION_IP_TO_RELAY_IP) {
                        P2Link.Direct directLinkOfRequestedName = new P2Link.Direct(to.relayLink.dnsOrIp, m1.nextInt());

                        //before we close we will send a packet to the link, just so we create a hole in our own firewall.. We need no response, as it is too likely that it at least reached our router - creating the hole.
                        parent.sendInternalMessage(directLinkOfRequestedName.resolve(), P2LMessage.createNatHolePacket());

                        P2LFuture<Boolean> reverseConnectionEstablishedFuture = waitForReverseConnection(parent, to, conversationId);

                        convo.close(); //now we can close and the remote can attempt a connection to our ip (not known to us, but known to the peer we used to relay) - an ip which should now have an NAT-entry for the remote

                        return reverseConnectionEstablishedFuture;
                    } else {
                        return new P2LFuture<>(false);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return new P2LFuture<>(false);
                }
            });
        }
    }
    private static P2LFuture<Boolean> waitForReverseConnection(P2LNodeInternal parent, P2Link.Relayed requiredFrom, short requiredConversationId) {
        if(parent.isConnectedTo(requiredFrom))
            return new P2LFuture<>(true);

        P2LFuture<Boolean> future = new P2LFuture<>();
        BiConsumer<P2LConnection, Integer> listener = (con, connectConversationId) -> {
            if(requiredConversationId == connectConversationId && requiredFrom.equals(con.link))
                future.trySetCompleted(true);
        };
        parent.addConnectionEstablishedListener(listener);
        future.callMeBack(b -> parent.removeConnectionEstablishedListener(listener));

        if(parent.isConnectedTo(requiredFrom))
            future.trySetCompleted(true);
        return future;
    }

    //SL_REQUEST_DIRECT_CONNECT_TO
    public static void asAnswerer_ConnectTo(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        convo.close();
        P2Link.Direct toConnectTo = (P2Link.Direct) P2Link.Direct.from(m0.asBytes());
        DirectConnectionProtocol.asInitiator(parent, toConnectTo.resolve(), convo.getConversationId());
    }

    //SL_REQUEST_DIRECT_CONNECT_TO_MODIFY_DESTINATION_IP_TO_RELAY_IP
    public static void asAnswerer_ConnectTo_modifyDestinationIpToRelayIp(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        convo.close();
        P2Link.Direct toConnectTo = new P2Link.Direct(convo.getPeer().getHostName(), m0.nextInt());
        DirectConnectionProtocol.asInitiator(parent, toConnectTo.resolve(), convo.getConversationId());
    }

    //SL_CONNECTION_RELAY
    public static void asAnswerer_RelayConnection(P2LNodeInternal parent, P2LConversation convo, P2LMessage m0) throws IOException {
        convo.setA(500);

        InetSocketAddress rawAddressRequesterPeer = convo.getPeer();
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
        System.out.println("rawAddressSecondPeer = " + rawAddressSecondPeer);
        System.out.println("rawAddressRequesterPeer = " + rawAddressRequesterPeer);

        //we do not inform the peer that requested the relay of the result, nor does the second peer inform us whether it accepted the request.
        //  the initially requesting peer will notice via timeout that no one connected to it.
        if(isRequestComingFromPubliclyAvailableLink) {
            //No need to NAT punch, requester is public
            convo.closeWith(convo.encode(EXPECT_INCOMING_CONNECTION));
            P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
            convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
        } else {
            boolean isRequesterPeerInLocalSubnet = NetUtil.isV4AndFromSameSubnet(rawAddressRequesterPeer.getAddress(), loadOrGetIp4InterfaceAddress());
            boolean isSecondPeerInLocalSubnet = NetUtil.isV4AndFromSameSubnet(rawAddressSecondPeer.getAddress(), loadOrGetIp4InterfaceAddress());

            if(isRequesterPeerInLocalSubnet == isSecondPeerInLocalSubnet) { //i.e. either both in WAN or both in LAN - i.e. nat punch is either not required but works anyways or is required and hopefully works(nat config)
                //NAT PUNCH - requester has already send punching packet(which will likely not be received by remote) - so now the remote can start sending
                convo.answerClose(convo.encode(CONTINUE_WITH_NAT_PUNCH, directLinkToSecondPeerBytes)); //THIS HAS TO COMPLETE BEFORE WE TELL SECOND PEER TO CONNECT - IT CONTAINS THE NAT PUNCH INITIAL MESSAGE

                //now that a nat hole exists in the requester's nat, we can simply initiate a connection to it from the second peer (which will create a nat hole with its first message)
                P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
                convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
            } else if(isSecondPeerInLocalSubnet) { //requires the server to be behind a symmetric nat
                //We still do the NAT punch, but we assume that our nat is SYMMETRIC and that it's outgoing port will be the same that we see here...
                //   however since it is in the same subnet we are, we let the remote modify the ip to ours - hoping that we and the second peer are behind the same nat
                convo.answerClose(convo.encode(CONTINUE_WITH_NAT_PUNCH_MODIFY_DESTINATION_IP_TO_RELAY_IP, rawAddressSecondPeer.getPort())); //THIS HAS TO COMPLETE BEFORE WE TELL SECOND PEER TO CONNECT - IT CONTAINS THE NAT PUNCH INITIAL MESSAGE

                //continue normally
                P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO, connectionId, rawAddressSecondPeer);
                convoWithSecondPeer.initClose(convoWithSecondPeer.encodeSingle(directLinkToRequesterPeerBytes));
            } else if(isRequesterPeerInLocalSubnet) { //requires the server to be behind a symmetric nat
                //the requester can do a normal nat punch
                convo.answerClose(convo.encode(CONTINUE_WITH_NAT_PUNCH, directLinkToSecondPeerBytes)); //THIS HAS TO COMPLETE BEFORE WE TELL SECOND PEER TO CONNECT - IT CONTAINS THE NAT PUNCH INITIAL MESSAGE

                //but the second peer will have to connect to a modified address, consisting of what it sees as source ip from our request and the port of the requester peer
                P2LConversation convoWithSecondPeer = parent.internalConvo(SL_REQUEST_DIRECT_CONNECT_TO_MODIFY_DESTINATION_IP_TO_RELAY_IP, connectionId, rawAddressSecondPeer);
                convoWithSecondPeer.initClose(convoWithSecondPeer.encode(rawAddressRequesterPeer.getPort()));
            }

        }
    }



    private static InterfaceAddress ip4InterfaceAddress = null;
    private static InterfaceAddress loadOrGetIp4InterfaceAddress() { //might take a while to instantiate - so we have to do it statically and lazily
        if(ip4InterfaceAddress == null) {
            synchronized (RelayedConnectionProtocol.class) {
                if (ip4InterfaceAddress == null) {
                    ip4InterfaceAddress = NetUtil.getLocalIPv4InterfaceAddress();
                }
            }
        }
        return ip4InterfaceAddress;
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
