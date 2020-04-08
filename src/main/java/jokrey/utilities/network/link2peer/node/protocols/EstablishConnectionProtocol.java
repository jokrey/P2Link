package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;

import java.net.InetSocketAddress;

/**
 * IDEA:
 *   a new node does only need an internet connection and know the link of a single node with a public link
 *   using only that it can become a node to which other nodes can connect
 *
 * Types of connection establishing:
 *  - Both pub links (simple connect + send own public link, public link of peer known)
 *  - Requester is hidden, answerer is public
 *      Simple connect, answerer serves as relay server for requester from now on
 *   - requester is public, answerer is hidden
 *      Requester asks relay server(always the one that the node hidden link was obtained from), to send answerer a CONNECT_TO_ME request with requersters public link
 *  - requester is hidden, answerer is hidden
 *      Requester asks relay server for direct information - simultaneously the relay server send the hole information of the answerer to the requester
 *      Requester attempts connection + answerer attempts connection
 *
 *
 * P2L:
 *     NAT Problem
 *         Clients that do not know their public ip cannot send it to other nodes (so that they use it when being asked about their peers)
 *         Instead they will tell the peer that they don't know
 *              The peer will then check the source ip of the packet and use that
 *              However if both nodes are in the same private network - this does not work
 *                  If a third peer is not in the same private network the ip would not be resolvable to them
 *                  If the second peer were to send a packet to that third peer, the third peer would see a different source ip (the public ip of the router/nat)
 *                  So in a way a single node can have multiple links(if it itself does not know its own public ip)
 *                  (connection denied, already connected - since source packet ip should be the same either way)
 *
 * Node will ask who am i to every new peer and send who they think the other one is


 //todo: mtu detection + exchange
 //todo - RE-establish connection protocol that does not do the nonce check - for historic connections (more efficient retry)
 //   todo - literally only a check if connection is in historic connections => then ping it...
 */
public class EstablishConnectionProtocol {
    /**
     * Either 'to' or resolved can be null. If resolved is not null a direct connection attempt will be made.
     */
    public static boolean asInitiator(P2LNodeInternal parent, P2Link to, InetSocketAddress resolved) {
        if(resolved != null) {
            return DirectConnectionProtocol.asInitiator(parent, resolved);
        } else if(to != null) {
            if(to.isRelayed()) {
                return RelayedConnectionProtocol.asInitiator(parent, (P2Link.Relayed) to);
            } else if(to.isOnlyLocal()) {
                System.err.println("Note: "+to+" is a local link, can only be used if to actually local and the link will not be distributed to peers - connect to local almost exclusively used during development");
                return DirectConnectionProtocol.asInitiator(parent, parent.resolve(to));
            } else {//if(to.isDirect()) {
                return DirectConnectionProtocol.asInitiator(parent, parent.resolve(to));
            }
        } else {
            throw new NullPointerException("both to and resolved were null");
        }
    }
}