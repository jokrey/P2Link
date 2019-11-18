package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LNodeInternal;
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
        getInputStream(message).received(message);
        //potentially a delayed package, after a new stream of the same type and conversation id has been created (very, very unlikely in context)
    }
    private P2LInputStream getInputStream(P2LMessage m) {
        return inputStreams.get(new SenderTypeConversationIdentifier(m));
    }
    public P2LOrderedInputStream createInputStream(P2LNodeInternal parent, SocketAddress from, int type, int conversationId) {
        if(from == null || parent == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(from, type, conversationId);
        return (P2LOrderedInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LOrderedInputStreamImplV1(parent, from, type, conversationId));
    }
    public P2LFragmentInputStream createFragmentInputStream(P2LNodeInternal parent, SocketAddress from, int type, int conversationId) {
        if(from == null || parent == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(from, type, conversationId);
        return (P2LFragmentInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LFragmentInputStreamImplV1(parent, from, type, conversationId));
    }



    private final ConcurrentHashMap<HeaderIdentifier, P2LOrderedOutputStream> outputStreams = new ConcurrentHashMap<>();
    public void receivedReceipt(P2LMessage rawReceipt) {
        getOutputStream(rawReceipt).receivedReceipt(rawReceipt);
    }
    private P2LOrderedOutputStream getOutputStream(P2LMessage m) {
        return outputStreams.get(new SenderTypeConversationIdentifier(m));
    }
    public P2LOrderedOutputStream getOutputStream(P2LNodeInternal parent, SocketAddress to, int type, int conversationId) {
        if(parent == null || to == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(to, type, conversationId);
        return outputStreams.computeIfAbsent(identifier, k -> new P2LOrderedOutputStreamImplV1(parent, to, type, conversationId));
    }
}
