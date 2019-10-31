package jokrey.utilities.network.link2peer.core;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class GarnerConnectionsRecursivelyProtocol {
    /**
     * Will establish a connection to every given setup link and request their peers in a BLOCKING fashion.
     *
     * From then it will recursively attempt to establish connections to randomly selected received peers, until the new connection limit is reached.
     * If the connection limit is smaller than the number of setup links, not all setup links may be connected to
     *
     * The max peer limit in the constructor is being respected at all times
     *
     * @param newConnectionLimit maximum number of new connections, after this limit is reached the algorithm will terminate
     * @param setupLinks links to begin discovering connections from, setup links are connected to and count as new connections in the newConnectionLimit
     * @return newly, successfully connected links
     */
    static List<SocketAddress> recursiveGarnerConnections(P2LNodeInternal parent, int newConnectionLimit, int newConnectionLimitPerRecursion, List<SocketAddress> setupLinks) {
        //also naturally limited by peerLimit set in constructor (and ram cap)

        if(setupLinks.isEmpty() || newConnectionLimit <=0)
            return Collections.emptyList();

        List<SocketAddress> connectedSetupLinks = new ArrayList<>(setupLinks.size());
        int newlyConnectedCounter = 0;
        for(SocketAddress peerLink : setupLinks) {
            if(newlyConnectedCounter >= newConnectionLimit || newlyConnectedCounter >= newConnectionLimitPerRecursion)
                return connectedSetupLinks;
            try {
                if(!parent.isConnectedTo(peerLink)) {
                    EstablishSingleConnectionProtocol.asInitiator(parent, peerLink);
                    newlyConnectedCounter++;
                }
                connectedSetupLinks.add(peerLink);
            } catch (IOException  e) {
                e.printStackTrace();
                System.err.println(peerLink+" could not be connected to, because: "+e.getMessage());
            }
        }

        List<SocketAddress> foundUnconnectedLinks = new ArrayList<>();
        for (int i = 0, connectedSetupLinksSize = connectedSetupLinks.size(); i < connectedSetupLinksSize; i++) {
            SocketAddress connectedSetupLink = connectedSetupLinks.get(i);
            try {
                String[] foundLinks = RequestPeerLinksProtocol.asInitiator(parent, connectedSetupLink);
                for (String foundLink : foundLinks) {
                    SocketAddress address = WhoAmIProtocol.fromString(foundLink);
                    if (!parent.isConnectedTo(address))
                        foundUnconnectedLinks.add(address);
                }
                if (i + 1 > 3 && foundUnconnectedLinks.size() > 2 * parent.remainingNumberOfAllowedPeerConnections()) { //fixme heuristic
                    //assume that one of 3 peers is honest and that at least half unconnected links are still valid...
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Collections.shuffle(foundUnconnectedLinks); // randomize which connections to establish first, to make it harder to isolate a peer

        connectedSetupLinks.addAll(recursiveGarnerConnections(parent,
                  newConnectionLimit - newlyConnectedCounter,
                                    newConnectionLimitPerRecursion,
                                    foundUnconnectedLinks));

        return connectedSetupLinks;
    }
}
