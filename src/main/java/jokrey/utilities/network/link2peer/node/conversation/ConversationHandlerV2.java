package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.network.link2peer.P2LMessage;
import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

public class ConversationHandlerV2 {
    private final Map<Short, ConversationAnswererChangeThisName> conversationHandlers = new ConcurrentHashMap<>();
    public void registerConversationHandlerFor(int type, ConversationAnswererChangeThisName handler) {
        conversationHandlers.put(toShort(type), handler);
    }

    private final Map<SenderTypeConversationIdentifier, P2LConversationImplV2> activeConversations = new ConcurrentHashMap<>();

    public void received(P2LNodeInternal parent, InetSocketAddress from, ReceivedP2LMessage received) throws IOException {
        if(!received.header.isReceipt() && received.header.getStep() == 0) {
            P2LConversationImplV2 convo = (P2LConversationImplV2) parent.internalConvo(received.header.getType(), received.header.getConversationId(), from);
            convo.notifyServerInit(conversationHandlers.get(received.header.getType()), received); //if was previous active, then the conversation can at least log that this is a double receival.
        } else {
            SenderTypeConversationIdentifier id = new SenderTypeConversationIdentifier(received);
            P2LConversationImplV2 convo = activeConversations.get(id);
            if(convo != null) //if the convo does not exist, then the received message will be considered old.
                convo.notifyReceived(received);
            else if(received.header.requestReceipt()) //i.e. our close message was lost and the other node has resent its last message
                parent.sendInternalMessage(from, received.createReceipt());
            else
                DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }
    public void clean() {
//        activeConversations.clear(); todo - how do we clean dormant active conversations?! Do they exist? Don't they time out?
    }

    public P2LConversation getConvoFor(P2LNodeInternal parent, P2LConnection con, InetSocketAddress to, short type, short conversationId) {
        return activeConversations.computeIfAbsent(new SenderTypeConversationIdentifier(to, type, conversationId),
                k -> new P2LConversationImplV2(parent, this, con, to, type, conversationId));
    }
    void remove(P2LConversationImplV2 convo) {
        activeConversations.remove(new SenderTypeConversationIdentifier(convo.peer, convo.type, convo.conversationId));
    }
}
