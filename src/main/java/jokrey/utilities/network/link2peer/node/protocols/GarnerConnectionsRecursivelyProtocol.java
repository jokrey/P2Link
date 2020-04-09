package jokrey.utilities.network.link2peer.node.protocols;

import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.network.link2peer.util.TimeoutException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public class GarnerConnectionsRecursivelyProtocol {
    /**
     * Will establish a connection to every given setup link and request their peers in a BLOCKING fashion.
     *
     * From then it will recursively attempt to establish connections to randomly selected received peers, until the new connection limit is reached.
     * If the connection limit is smaller than the number of setup links, not all setup links may be connected to
     *
     * The max peer limit in the constructor is being respected at all times
     *
     * @param newConnectionGoal maximum number of new connections, after this limit is reached the algorithm will terminate(however more new connections may have been made at that point) - just used as a rough estimate byte
     * @param setupLinks links to begin discovering connections from, setup links are connected to and count as new connections in the newConnectionLimit
     * @return newly, successfully connected links
     */
    public static P2LFuture<List<P2Link>> recursiveGarnerConnections(P2LNodeInternal parent, int newConnectionGoal, int newConnectionGoalPerRecursion, List<P2Link> setupLinks) {
//        //also naturally limited by peerLimit set in constructor (and ram cap, but what isn't)
        int goal = Math.min(newConnectionGoal, newConnectionGoalPerRecursion);

        if(setupLinks.isEmpty() || newConnectionGoal <=0)
            return new P2LFuture<>(Collections.emptyList());
        if(setupLinks.size() > goal)
            throw new IllegalArgumentException("cannot have more setup links that the limit(min(newConnectionGoal, newConnectionGoalPerRecursion))");

        return parent.establishConnections(setupLinks.toArray(new P2Link[0])).andThen(connectedSetupLinks -> {
            Collections.shuffle(connectedSetupLinks);

            int numberConnected = connectedSetupLinks.size();
            int remaining = goal - numberConnected;
            if(remaining <= 0)
                return new P2LFuture<>(connectedSetupLinks);
            else {
                List<P2LFuture<List<P2Link>>> requests = new LinkedList<>();

                for (P2Link connectedSetupLink : connectedSetupLinks) {
                    InetSocketAddress resolvedConnectedSetupLink = parent.resolve(connectedSetupLink);
                    requests.add(RequestPeerLinksProtocol.asInitiator(parent, resolvedConnectedSetupLink));
                }

                return P2LFuture.oneForAll(requests).andThen(lists -> {
                    List<P2Link> foundUnconnectedLinks = new ArrayList<>();
                    for (List<P2Link> list : lists)
                        for (P2Link link : list)
                            if (!parent.isConnectedTo(link))
                                foundUnconnectedLinks.add(link);
                    Collections.shuffle(foundUnconnectedLinks);
                    return recursiveGarnerConnections(parent, remaining, newConnectionGoalPerRecursion, takeAtMost(foundUnconnectedLinks, Math.min(remaining, (int) (newConnectionGoalPerRecursion*1.5/*fixme heuristic*/)))).toType(recursivelyConnected -> {
                        connectedSetupLinks.addAll(recursivelyConnected);
                        return connectedSetupLinks;
                    });
                });
            }
        });
    }

    private static <E> List<E> takeAtMost(List<E> list, int num) {
        int limit = Math.min(num, list.size());
        ArrayList<E> r = new ArrayList<>(limit);
        for(int i=0;i<limit;i++)
            r.add(list.get(i));
        return r;
    }
}
