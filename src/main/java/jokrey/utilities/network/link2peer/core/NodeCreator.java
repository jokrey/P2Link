package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;

public class NodeCreator {
    public static P2LNode create(P2Link selfLink) {return new P2LNodeImpl(selfLink); }
    public static P2LNode create(P2Link selfLink, int peerLimit) {return new P2LNodeImpl(selfLink, peerLimit); }
}
