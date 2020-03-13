package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

//TODO
class ConversationHandlerV2 {
    private final Map<Short, ConversationAnswererChangeThisName> conversationHandlers = new ConcurrentHashMap<>();
    void registerConversationHandlerFor(int type, ConversationAnswererChangeThisName handler) {
        conversationHandlers.put(toShort(type), handler);
    }

    private final Map<SenderTypeConversationIdentifier, P2LConversationImplV2> activeConversations = new ConcurrentHashMap<>();

    void received(P2LNodeInternal parent, SocketAddress from, P2LMessage received) throws IOException {
        if(!received.header.isReceipt() && received.header.getStep() == 0) {
            P2LConversationImplV2 convo = (P2LConversationImplV2) parent.internalConvo(received.header.getType(), received.header.getConversationId(), from);
            convo.notifyServerInit(conversationHandlers.get(received.header.getType()), received); //if was previous active, then the conversation can at least log that this is a double receival.
        } else {
            SenderTypeConversationIdentifier id = new SenderTypeConversationIdentifier(received);
            P2LConversationImplV2 convo = activeConversations.get(id);
            System.out.println(parent.getSelfLink() + " - received("+id+") = " + received+" - convo="+convo);
            if(convo != null) //if the convo does not exist, then the received message will be considered old.
                convo.notifyReceived(received);
            else if(received.header.requestReceipt()) //i.e. our close message was lost and the other node has resent its last message
                parent.sendInternalMessage(from, received.createReceipt());
            else
                DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }
    void clean() {
//        activeConversations.clear(); todo - how do we clean dormant active conversations
    }

    P2LConversation getConvoFor(P2LNodeInternal parent, P2LConnection con, SocketAddress to, short type, short conversationId) {
        return activeConversations.computeIfAbsent(new SenderTypeConversationIdentifier(to, type, conversationId),
                k -> new P2LConversationImplV2(parent, this, con, to, type, conversationId));
    }
    void remove(P2LConversationImplV2 convo) {
        System.out.println(convo.parent.getSelfLink() + " remove - "+convo);
        activeConversations.remove(new SenderTypeConversationIdentifier(convo.peer, convo.type, convo.conversationId));
    }
}