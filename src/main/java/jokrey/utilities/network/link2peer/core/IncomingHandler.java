package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.mcnp.io.ConnectionHandler;
import jokrey.utilities.network.mcnp.io.MCNP_ConnectionIO;
import jokrey.utilities.network.mcnp.io.MCNP_ServerIO;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

import static jokrey.utilities.network.link2peer.core.P2L_Message_IDS.*;

/**
 * @author jokrey
 */
class IncomingHandler {
    private P2LNodeInternal parent;
    private MCNP_ServerIO<HandledPeerConnection> server;

    P2LMessageQueue internalMessageQueue = new P2LMessageQueue();
    P2LMessageQueue userIdvMessageQueue = new P2LMessageQueue();
    P2LMessageQueue userBrdMessageQueue = new P2LMessageQueue();

    public IncomingHandler(P2LNodeInternal parentG) {
        this.parent = parentG;
        server = new MCNP_ServerIO<>(parent.getSelfLink().port, new ConnectionHandler<HandledPeerConnection>() {
            @Override public HandledPeerConnection newConnection(int initial_cause, MCNP_ConnectionIO connection) throws IOException {
                if(initial_cause == IC_CONNECTION_REQUEST_LINK_VERIFY) {
                    //ok attack description(hindered by verificationRequestAllowances as implemented here):
                    //   an attacker attempts to connect to a node WITHOUT providing a public link that others can use to connect to them
                    //   they connect to a node and provide the link of a third node - masquerading as that third client
                    //   the third node would just answer the request without thinking twice
                    //   the original node would believe the connection it has is to the link of the third node
                    //       the verification ensures this does not happen. The third node would have to have a current connection attempt to the first node at that EXACT time.
                    // question: should nodes be allowed to connect without providing a public link, as long as explicitly state that?
                    //   some kind of 'light peer'?

                    P2Link peerSaysItsLinkIs = new P2Link(connection.receive_utf8(200)); //fixme heuristic
                    if(verificationRequestAllowedFor(peerSaysItsLinkIs))
                        connection.send_utf8(parent.getSelfLink().raw);
                } else if(initial_cause == IC_NEW_PEER_CONNECTION) {
                    if(!parent.maxPeersReached()) {
                        PeerConnection peerConnection = EstablishSingleConnectionProtocol.asReceiver(parent, connection);
                        if (peerConnection != null) {
                            parent.addActiveIncomingPeer(peerConnection);
                            return new HandledPeerConnection(IC_NEW_PEER_CONNECTION, peerConnection);
                        }
                    }
                }
//                else if (initial_cause == IC_REQUEST_KNOWN_ACTIVE_PEER_LINKS) { //todo - not currently supported for mitigation of potential dos, and unlikely usage
//                    RequestPeerLinksProtocol.answerRequest(connection, parent.getActivePeerLinks());
//                }

                connection.close();
                return null;
            }

            private BroadcastMessageProtocol.BroadcastState broadcastState = new BroadcastMessageProtocol.BroadcastState();
            @Override public HandledPeerConnection handleInteraction(TypedCause type_cause, MCNP_ConnectionIO rawCon, HandledPeerConnection state) throws IOException {
//                System.err.println("IncomingHandler.handleInteraction");
                if(type_cause.getType() != IC_NEW_PEER_CONNECTION) throw new IllegalStateException("must be valid peer connection");

                //error description::
                //  the opposing connection sends a cause, meanwhile (simultaneously) this connection begins a

                //both connections are in the receiving state at mcnp-server cause
                //  one connection (c1) begins by sending a cause - the other connection (c2) can receive data from now on, BUT CANNOT SEND DATA!! - since c1 is still in cause receive state

                P2Link from = state.connection.peerLink;

                int msgType = type_cause.getCause();
//                System.err.println("handleInteraction - msgType = " + msgType);

                if(msgType == SC_BROADCAST) {
//                    System.err.println("SC_BROADCAST");
                    new Thread(() -> { //todo - dos mitigation:: could be exploited by sending many broadcast supercauses, but not sending anything else.. (the thread would hang)
                        try {
                            P2LMessage received = BroadcastMessageProtocol.receive(broadcastState, parent, state.connection);
                            if(received != null) {
                                userBrdMessageQueue.handleNewMessage(received);
                                parent.notifyBroadcastMessageReceived(received);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }).start();
                } else if(msgType == SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS) {
                    new Thread(() -> {
                        try {
                            RequestPeerLinksProtocol.answerRequest(state.connection, parent.getActivePeerLinks());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }).start();
                } else {
                    byte[] rawMessage = rawCon.receive_variable();
                    P2LMessage message = new P2LMessage(from, msgType, rawMessage);
                    if(isInternalMessageId(msgType)) {
                        internalMessageQueue.handleNewMessage(message);
                    } else {
                        userIdvMessageQueue.handleNewMessage(message);
                        parent.notifyIndividualMessageReceived(message);
                    }
                }

                return state;
            }

            @Override public void connectionDropped(MCNP_ConnectionIO conn, HandledPeerConnection state, boolean eof) {
                System.err.println("IncomingHandler.connectionDropped - self: "+parent.getSelfLink()+", con to: "+(state==null?"directly dropped":state.connection.peerLink));
                if(state != null) {
                    parent.markBrokenConnection(state.connection.peerLink);
                }
            }
            @Override public void connectionDroppedWithError(Throwable t, MCNP_ConnectionIO conn, HandledPeerConnection state) {
                System.err.println("IncomingHandler.connectionDroppedWithError("+t.getMessage()+") - self: "+parent.getSelfLink()+", con to: "+(state==null?"directly dropped":state.connection.peerLink));
                if(state != null) {
                    parent.markBrokenConnection(state.connection.peerLink);
                }
            }
        });
    }

    protected void startListenerThread() {
        server.runListenerLoopInThread();
    }

    public void listenToOutgoing(PeerConnection established) {
        server.handleExternalInitializedConnection(new HandledPeerConnection(IC_NEW_PEER_CONNECTION, established), IC_NEW_PEER_CONNECTION, (MCNP_ConnectionIO) established.raw());
    }

    private final CopyOnWriteArraySet<P2Link> verificationRequestPermissions = new CopyOnWriteArraySet<>();
    public void addLinkVerificationRequestPermission(P2Link link) {
        if(! link.equals(parent.getSelfLink()))
            verificationRequestPermissions.add(link);
    }
    private boolean verificationRequestAllowedFor(P2Link link) {
        return verificationRequestPermissions.remove(link);
    }
    public boolean revokeVerificationRequestPermission(P2Link link) {
        return verificationRequestPermissions.remove(link);
    }

    class HandledPeerConnection extends ConnectionHandler.ConnectionState {
        public final PeerConnection connection;
        public HandledPeerConnection(int connection_type, PeerConnection connection) {
            super(connection_type);
            this.connection = connection;
        }
    }
}
