package jokrey.utilities.network.link2peer.node.stream;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.HeaderIdentifier;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.SenderTypeConversationIdStepIdentifier;

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
        return inputStreams.get(new SenderTypeConversationIdStepIdentifier(m));
    }
    public P2LOrderedInputStream createInputStream(P2LNodeInternal parent, SocketAddress from, P2LConnection con, short type, short conversationId, short step) {
        if(from == null || parent == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdStepIdentifier(from, type, conversationId, step);
//        return (P2LOrderedInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LOrderedInputStreamImplV1(parent, from, type, conversationId));
        return (P2LOrderedInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LOrderedInputStreamImplV2(parent, from, con, type, conversationId, step));
    }
    public P2LFragmentInputStream createFragmentInputStream(P2LNodeInternal parent, SocketAddress from, P2LConnection con, short type, short conversationId, short step) {
        if(from == null || parent == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdStepIdentifier(from, type, conversationId, step);
        return (P2LFragmentInputStream) inputStreams.computeIfAbsent(identifier, k -> new P2LFragmentInputStreamImplV1(parent, from, con, type, conversationId, step));
    }
    public boolean createCustomInputStream(SocketAddress from, short type, short conversationId, short step, P2LInputStream inputStream) {
        if(from == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdStepIdentifier(from, type, conversationId, step);
        return inputStreams.putIfAbsent(identifier, inputStream) == null;
    }



    private final ConcurrentHashMap<HeaderIdentifier, P2LOutputStream> outputStreams = new ConcurrentHashMap<>();
    public void receivedReceipt(P2LMessage rawReceipt) {
        P2LOutputStream stream = getOutputStream(rawReceipt);
        if(stream != null)
            stream.receivedReceipt(rawReceipt);
        else
            System.out.println("received receipt message for unknown out stream("+new SenderTypeConversationIdStepIdentifier(rawReceipt)+")"); //todo - default handling?
    }
    private P2LOutputStream getOutputStream(P2LMessage m) {
        return outputStreams.get(new SenderTypeConversationIdStepIdentifier(m));
    }
    public P2LOrderedOutputStream createOutputStream(P2LNodeInternal parent, SocketAddress to, P2LConnection con, short type, short conversationId, short step) {
        if(parent == null || to == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdStepIdentifier(to, type, conversationId, step);
//        return (P2LOrderedOutputStream) outputStreams.computeIfAbsent(identifier, k -> new P2LOrderedOutputStreamImplV1(parent, to, con, type, conversationId));
        return (P2LOrderedOutputStream) outputStreams.computeIfAbsent(identifier, k -> new P2LOrderedOutputStreamImplV2(parent, to, con, type, conversationId, step));
    }
    public P2LFragmentOutputStream createFragmentOutputStream(P2LNodeInternal parent, SocketAddress to, P2LConnection con, short type, short conversationId, short step) {
        if(parent == null || to == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdStepIdentifier(to, type, conversationId, step);
        return (P2LFragmentOutputStream) outputStreams.computeIfAbsent(identifier, k -> new P2LFragmentOutputStreamImplV1(parent, to, con, type, conversationId, step));
    }
    public boolean registerCustomOutputStream(SocketAddress from, short type, short conversationId, short step, P2LOutputStream outputStream) {
        if(from == null) throw new NullPointerException();
        HeaderIdentifier identifier = new SenderTypeConversationIdStepIdentifier(from, type, step, conversationId);
        return outputStreams.putIfAbsent(identifier, outputStream) == null;
    }

    public void unregister(P2LOutputStream stream) {
        outputStreams.remove(new SenderTypeConversationIdStepIdentifier(stream.getRawFrom(), stream.getType(), stream.getConversationId(), stream.getStep()));
    }
    /** TODO -  Problem: if unregister is instantly called on close - if the close receipt fails the output stream may never know it had succeeded. */
    public void unregister(P2LInputStream stream) {
        System.out.println("StreamMessageHandler.unregister - stream = " + stream);
        inputStreams.remove(new SenderTypeConversationIdStepIdentifier(stream.getRawFrom(), stream.getType(), stream.getConversationId(), stream.getStep()));
    }
}
