package jokrey.utilities.network.link2peer;

import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.util.Hash;

import java.net.InetSocketAddress;

public class ReceivedP2LMessage extends P2LMessage {
    public final InetSocketAddress sender;
    public ReceivedP2LMessage(InetSocketAddress sender, P2LMessageHeader header, Hash contentHash, byte[] raw, int payloadLength) {
        super(header, contentHash, raw, payloadLength);
        this.sender = sender;
    }
}
