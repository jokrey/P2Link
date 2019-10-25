package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2Link;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class GarnerConnectionsRecursivelyProtocol {
    /**
     * Will establish a connection to every given setup link and request their peers.
     *
     * From then it will recursively attempt to establish connections to randomly selected received peers, until the new connection limit is reached.
     * If the connection limit is smaller than the number of setup links, not all setup links may be connected to
     *
     * The max peer limit in the constructor is being respected at all times
     *
     * @param newConnectionLimit
     * @param setupLinks
     * @return newly, successfully connected links
     */
    public static List<P2Link> recursiveGarnerConnections(P2LNodeInternal parent, int newConnectionLimit, int newConnectionLimitPerRecursion, List<P2Link> setupLinks) {
        //also naturally limited by peerLimit set in constructor (and ram cap)

        if(setupLinks.isEmpty() || newConnectionLimit <=0)
            return Collections.emptyList();

        List<P2Link> connectedSetupLinks = new ArrayList<>(setupLinks.size());
        int newlyConnectedCounter = 0;
        for(P2Link peerLink : setupLinks) {
            if(newlyConnectedCounter >= newConnectionLimit || newlyConnectedCounter >= newConnectionLimitPerRecursion)
                return connectedSetupLinks;
            try {
                if(!parent.getSelfLink().equals(peerLink) && ! parent.isConnectedTo(peerLink)) {
                    EstablishSingleConnectionProtocol.asRequester(parent, peerLink);
                    newlyConnectedCounter++;
                }
                connectedSetupLinks.add(peerLink);
            } catch (IOException  e) {
                e.printStackTrace();
                System.err.println(peerLink+" could not be connected to, because: "+e.getMessage());
            }
        }

        List<P2Link> foundUnconnectedLinks = new ArrayList<>();
        for (int i = 0, connectedSetupLinksSize = connectedSetupLinks.size(); i < connectedSetupLinksSize; i++) {
            P2Link connectedSetupLink = connectedSetupLinks.get(i);
            P2Link[] foundLinks = requestPeersFromActive(parent, connectedSetupLink);
            for (P2Link foundLink : foundLinks)
                if (!parent.getSelfLink().equals(foundLink) && !parent.isConnectedTo(foundLink))
                    foundUnconnectedLinks.add(foundLink);
            if (i+1>3 && foundUnconnectedLinks.size() > 2 * parent.remainingNumberOfAllowedPeerConnections()) { //fixme heuristic
                //assume that one of 3 peers is honest and that at least half unconnected links are valid...
                break;
            }
        }

        Collections.shuffle(foundUnconnectedLinks); // randomize which connections to establish first, to make it harder to isolate a peer

        connectedSetupLinks.addAll(recursiveGarnerConnections(parent,
                  newConnectionLimit - newlyConnectedCounter,
                                    newConnectionLimitPerRecursion,
                                    foundUnconnectedLinks));

        return connectedSetupLinks;
    }
    private static P2Link[] requestPeersFromActive(P2LNodeInternal parent, P2Link peer) {
        SocketAddress peerConnection = parent.getActiveConnection(peer);
        if(peerConnection == null) {
            System.err.println("connection to " + peer + " not established - :("); //should never occur, new links should not loose connection instantly
            //cannot throw an exception, otherwise an exploit would be to let a searching peer establish a connection and drop it, causing the peer connection to be marked as broken and not connected - if an exception would be thrown here it would cancel the entire search
            return new P2Link[0];
        }
        try {
            return RequestPeerLinksProtocol.requestPeersFrom(parent, peer);
        } catch (IOException e) {
            //cannot throw an exception, otherwise an exploit would be to let a searching peer establish a connection and drop it, causing the peer connection to be marked as broken and not connected - if an exception would be thrown here it would cancel the entire search
            return new P2Link[0];
        }
    }
}
