package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

public class ConversationHandler {
    private final Map<Short, ConversationReceivalHandlerChangeThisName> conversationHandlers = new ConcurrentHashMap<>();
    public void registerConversationFor(int type, ConversationReceivalHandlerChangeThisName handler) {
        conversationHandlers.put(toShort(type), handler);
    }
    
    final P2LMessageQueue conversationQueue = new P2LMessageQueue();

    void received(P2LNodeInternal parent, SocketAddress from, P2LMessage received) throws IOException {
        if(received.header.getStep() == 0) {//todo - what if wrong retry?? i.e. m2 fails to be delivered?? Then we suddenly have two conversations...
            ConversationReceivalHandlerChangeThisName handler = conversationHandlers.get(received.header.getType());
            if (handler != null) {
                P2LConversation servingConvo = parent.convo(received.header.getType(), received.header.getConversationId(), from);
                servingConvo.serverInit();
                handler.converse(servingConvo, received);
            }
        } else {
            boolean hasBeenHandled = conversationQueue.handleNewMessage(received);
            if(!hasBeenHandled && received.header.requestReceipt()) // if this is the last message in a convo - but the convo does not (or no longer) exists
                parent.sendInternalMessage(from, received.createReceipt());
        }
    }
    void clean() {
        conversationQueue.clean();
    }

}
