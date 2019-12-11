package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LNode;
import jokrey.utilities.network.link2peer.P2Link;
import jokrey.utilities.network.link2peer.node.core.P2LNodeImpl;

import java.io.IOException;

public class NodeCreator {
    public static P2LNode create(P2Link selfLink) throws IOException {
        return new P2LNodeImpl(selfLink);
    }
    public static P2LNode create(P2Link selfLink, int peerLimit) throws IOException {
        return new P2LNodeImpl(selfLink, peerLimit);
    }
}
