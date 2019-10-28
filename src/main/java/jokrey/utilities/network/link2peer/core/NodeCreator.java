package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LNode;

import java.io.IOException;

public class NodeCreator {
    public static P2LNode create(int port) throws IOException {
        return new P2LNodeImpl(port);
    }
    public static P2LNode create(int port, int peerLimit) throws IOException {
        return new P2LNodeImpl(port, peerLimit);
    }
}
