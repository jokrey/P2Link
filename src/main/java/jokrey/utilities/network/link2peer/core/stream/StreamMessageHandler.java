package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.core.WhoAmIProtocol;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.HeaderIdentifier;
import jokrey.utilities.network.link2peer.core.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jokrey
 */
public class StreamMessageHandler {
    private final ConcurrentHashMap<HeaderIdentifier, P2LInputStream> inputStreams = new ConcurrentHashMap<>();

    public void received(P2LMessage message) {
        P2LInputStream stream = getInputStream(message);
        stream.received(message);

//        if(stream.isClosed())
//            inputStreams.remove(new SenderTypeConversationIdentifier(message));
        //todo PROBLEM: if a delayed package comes in after a request to resend a new inputStreams entry would be created
        //   solution: same last packet received time + a stream timeout feature (then this node app could control when data is cleaned up)
        //             additionally require that for a packet to be accepted,
        //                 it is required that the input stream is read from currently (i.e. someone is waiting/or rather a stream was requested and is not closed) - any other packages are disregarded
    }

    public P2LInputStream getInputStream(P2LNodeInternal parent, SocketAddress from, int type, int conversationId) {
        return getInputStream(new SenderTypeConversationIdentifier(WhoAmIProtocol.toString(from), type, conversationId), parent, from, type, conversationId);
    }
    private P2LInputStream getInputStream(P2LMessage m) {
        return getInputStream(new SenderTypeConversationIdentifier(m), null, null, 0, 0);
    }
    private P2LInputStream getInputStream(HeaderIdentifier identifier, P2LNodeInternal parent, SocketAddress from, int type, int conversationId) {
        return inputStreams.computeIfAbsent(identifier, k -> {
            if(from == null || parent == null) throw new NullPointerException();
            return new P2LInputStreamV1(parent, from, type, conversationId);
        });
    }



    private final ConcurrentHashMap<HeaderIdentifier, P2LOutputStream> outputStreams = new ConcurrentHashMap<>();
    /**
     *
     * @param parent
     * @param to
     * @param type
     * @param conversationId
     * @return
     * @throws IllegalStateException if the type and conversationId combination is already occupied
     */
    public P2LOutputStream getOutputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        return outputStreams.computeIfAbsent(new SenderTypeConversationIdentifier(WhoAmIProtocol.toString(to), type, conversationId), k -> new P2LOutputStreamV1(parent, to, type, conversationId));
    }
}
