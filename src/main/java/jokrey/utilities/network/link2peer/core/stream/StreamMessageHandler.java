package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.HeaderIdentifier;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal use only
 * @author jokrey
 */
public class StreamMessageHandler {
    private final ConcurrentHashMap<HeaderIdentifier, P2LInputStream> inputStreams = new ConcurrentHashMap<>();
    public void receivedPart(P2LMessage message) {
        P2LInputStream stream = getInputStream(message);
        stream.received(message);

//        if(stream.isClosed())
//            inputStreams.remove(new SenderTypeConversationIdentifier(message));
        //todo PROBLEM: if a delayed package comes in after a request to resend a new inputStreams entry would be created
        //   solution: same last packet received time + a stream timeout feature (then this node app could control when data is cleaned up)
        //             additionally require that for a packet to be accepted,
        //                 it is required that the input stream is read from currently (i.e. someone is waiting/or rather a stream was requested and is not closed) - any other packages are disregarded
    }
    private P2LInputStream getInputStream(P2LMessage m) {
        return getInputStream(null, new SenderTypeConversationIdentifier(m),null);
    }
    public P2LInputStream getInputStream(P2LNodeInternal parent, SocketAddress from, int type, int conversationId) {
        return getInputStream(parent, new SenderTypeConversationIdentifier(WhoAmIProtocol.toString(from), type, conversationId), from);
    }
    private P2LInputStream getInputStream(P2LNodeInternal parent, SenderTypeConversationIdentifier identifier, SocketAddress from) {
        return inputStreams.computeIfAbsent(identifier, k -> {
            if(from == null || parent == null) throw new NullPointerException();
            return new P2LInputStreamV1(parent, from, identifier.messageType, identifier.conversationId);
        });
    }



    private final ConcurrentHashMap<HeaderIdentifier, P2LOutputStream> outputStreams = new ConcurrentHashMap<>();
    public void receivedReceipt(P2LMessage rawReceipt) {
        getOutputStream(null, new SenderTypeConversationIdentifier(rawReceipt),null).receivedReceipt(rawReceipt);
    }
    public P2LOutputStream getOutputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        return getOutputStream(parent, new SenderTypeConversationIdentifier(WhoAmIProtocol.toString(to), type, conversationId), to);
    }
    private P2LOutputStream getOutputStream(P2LNodeInternal parent, SenderTypeConversationIdentifier identifier, SocketAddress to) {
        return outputStreams.computeIfAbsent(identifier, k -> {
            if(parent == null || to == null) throw new NullPointerException();
            return new P2LOutputStreamV1(parent, to, identifier.messageType, identifier.conversationId);
        });
    }
}
