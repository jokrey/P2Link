package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.core.IncomingHandler;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

class ConversationHandlerV1 implements ConversationHandler {
    private IncomingHandler incomingHandler;
    ConversationHandlerV1(IncomingHandler incomingHandler) {
        this.incomingHandler = incomingHandler;
    }

    private final Map<Short, ConversationAnswererChangeThisName> conversationHandlers = new ConcurrentHashMap<>();
    @Override public void registerConversationHandlerFor(int type, ConversationAnswererChangeThisName handler) {
        conversationHandlers.put(toShort(type), handler);
    }

    @Override public void received(P2LNodeInternal parent, InetSocketAddress from, ReceivedP2LMessage received) throws IOException {
        boolean hasBeenHandled = incomingHandler.messageQueue.handleNewMessage(received);
        System.out.println("received = " + received+" - hasBeenHandled="+hasBeenHandled);
        if(!hasBeenHandled) {
            if(!received.header.isReceipt() && received.header.getStep() == 0) {//to-do - potentially only allow greater(->newer) conversation id's for a peer-type combination,  - circular conversation id problem
                ConversationAnswererChangeThisName handler = conversationHandlers.get(received.header.getType());
                if (handler != null) {
                    P2LConversationImplV1 servingConvo = (P2LConversationImplV1) parent.internalConvo(received.header.getType(), received.header.getConversationId(), from);
                    servingConvo.serverInit(received);
                    handler.converse(servingConvo, received);
                }
            } else {
                if(received.header.requestReceipt()) // if this is the last message in a convo - but the convo does not (or no longer) exists
                    parent.sendInternalMessage(from, received.createReceipt());
            }
        }
    }
    @Override public void clean() { }

    @Override public P2LConversation getOutgoingConvoFor(P2LNodeInternal parent, P2LConnection con, InetSocketAddress to, short type, short conversationId) {
        return new P2LConversationImplV1(parent, incomingHandler.messageQueue, con, to, type, conversationId);
    }

    @Override public void remove(P2LConversationImplV2 convo) {

    }
}
