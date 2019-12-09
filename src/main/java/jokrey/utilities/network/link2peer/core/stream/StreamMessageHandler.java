package jokrey.utilities.network.link2peer.core.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.core.P2LConnection;
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
        P2LInputStream stream = getInputStream(message);
        if(stream != null)
            stream.received(message);
        else
            System.out.println("received message for unknown in stream"); //todo - default handling?
        //potentially a delayed package, after a new stream of the same type and conversation id has been created (very, very unlikely in context)
    }
    private P2LInputStream getInputStream(P2LMessage m) {
        return inputStreams.get(new SenderTypeConversationIdentifier(m));
    }
    public P2LOrderedInputStream createInputStream(P2LNodeInternal parent, SocketAddress from, P2LConnection con, int type, int conversationId) {
        if(from == null || parent == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(from, type, conversationId);
//        return (P2LOrderedInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LOrderedInputStreamImplV1(parent, from, type, conversationId));
        return (P2LOrderedInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LOrderedInputStreamImplV2(parent, from, con, type, conversationId));
    }
    public P2LFragmentInputStream createFragmentInputStream(P2LNodeInternal parent, SocketAddress from, P2LConnection con, int type, int conversationId) {
        if(from == null || parent == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(from, type, conversationId);
        return (P2LFragmentInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LFragmentInputStreamImplV1(parent, from, con, type, conversationId));
    }
    public boolean createCustomInputStream(SocketAddress from, int type, int conversationId, P2LInputStream inputStream) {
        if(from == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(from, type, conversationId);
        return inputStreams.putIfAbsent(identifier, inputStream) == null;
    }



    private final ConcurrentHashMap<HeaderIdentifier, P2LOutputStream> outputStreams = new ConcurrentHashMap<>();
    public void receivedReceipt(P2LMessage rawReceipt) {
        P2LOutputStream stream = getOutputStream(rawReceipt);
        if(stream != null)
            stream.receivedReceipt(rawReceipt);
        else
            System.out.println("received receipt message for unknown out stream("+new SenderTypeConversationIdentifier(rawReceipt)+")"); //todo - default handling?
    }
    private P2LOutputStream getOutputStream(P2LMessage m) {
        return outputStreams.get(new SenderTypeConversationIdentifier(m));
    }
    public P2LOrderedOutputStream createOutputStream(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId) {
        if(parent == null || to == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(to, type, conversationId);
//        return (P2LOrderedOutputStream) outputStreams.computeIfAbsent(identifier, k -> new P2LOrderedOutputStreamImplV1(parent, to, con, type, conversationId));
        return (P2LOrderedOutputStream) outputStreams.computeIfAbsent(identifier, k -> new P2LOrderedOutputStreamImplV2(parent, to, con, type, conversationId));
    }
    public P2LFragmentOutputStream createFragmentOutputStream(P2LNodeInternal parent, SocketAddress to, P2LConnection con, int type, int conversationId) {
        if(parent == null || to == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(to, type, conversationId);
        return (P2LFragmentOutputStream) outputStreams.computeIfAbsent(identifier, k -> new P2LFragmentOutputStreamImplV1(parent, to, con, type, conversationId));
    }
    public boolean registerCustomOutputStream(SocketAddress from, int type, int conversationId, P2LOutputStream outputStream) {
        if(from == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdentifier(from, type, conversationId);
        return outputStreams.putIfAbsent(identifier, outputStream) == null;
    }

    public void unregister(P2LOutputStream stream) {
        outputStreams.remove(new SenderTypeConversationIdentifier(stream.getRawFrom(), stream.getType(), stream.getConversationId()));
    }
    /** TODO -  Problem: if unregister is instantly called on close - if the close receipt fails the output stream may never know it had success. */
    public void unregister(P2LInputStream stream) {
        inputStreams.remove(new SenderTypeConversationIdentifier(stream.getRawFrom(), stream.getType(), stream.getConversationId()));
    }
}
