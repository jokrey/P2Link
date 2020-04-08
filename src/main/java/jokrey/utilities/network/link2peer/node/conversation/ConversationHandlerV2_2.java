package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.DebugStats;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader;
import jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.SenderTypeConversationIdentifier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static jokrey.utilities.network.link2peer.node.message_headers.P2LMessageHeader.toShort;

/**
 * Finally fixes the issue of two concurrent conversation that occur with the same conversation id at the exact same time.
 * Then both conversations are blocked in the client/server position and neither will never receive data.
 */
public class ConversationHandlerV2_2 implements ConversationHandler {
    private final Map<Short, ConversationAnswererChangeThisName> conversationHandlers = new ConcurrentHashMap<>();
    public void registerConversationHandlerFor(int type, ConversationAnswererChangeThisName handler) {
        conversationHandlers.put(toShort(type), handler);
    }

    private final Map<SenderTypeConversationIdentifier, P2LConversationImplV2> activeIncomingConversations = new ConcurrentHashMap<>();
    private final Map<SenderTypeConversationIdentifier, P2LConversationImplV2> activeOutgoingConversations = new ConcurrentHashMap<>();

    public void received(P2LNodeInternal parent, InetSocketAddress from, ReceivedP2LMessage received) throws IOException {
        System.out.println(parent.getSelfLink()+ " - from = " + from+", received = " + received);
        SenderTypeConversationIdentifier id = new SenderTypeConversationIdentifier(received);
        if(!received.header.isReceipt() && received.header.getStep() == 0) {
            P2LConversationImplV2 convo = activeIncomingConversations.computeIfAbsent(id, k ->
                    new P2LConversationImplV2(parent, this, parent.getConnection(from), from, received.header.getType(), received.header.getConversationId()));
            convo.notifyServerInit(conversationHandlers.get(received.header.getType()), received); //if was previous active, then the conversation can at least log that this is a double receival.
        } else {
            P2LConversationImplV2 convo = isServer(received.header) ? activeIncomingConversations.get(id) : activeOutgoingConversations.get(id);
            if(convo != null) //if the convo does not exist, then the received message will be considered old.
                convo.notifyReceived(received);
            else if(received.header.requestReceipt()) //i.e. our close message was lost and the other node has resent its last message
                parent.sendInternalMessage(from, received.createReceipt());
            else
                DebugStats.conversation_numDoubleReceived.getAndIncrement();
        }
    }

    public static boolean isServer(P2LMessageHeader header) {
        return (header.getStep() & 1) == (header.isReceipt()?1:0);
    }

    public void clean() {
//        activeConversations.clear(); how do we clean dormant active conversations?! Do they exist? Don't they time out? They timeout...
    }

    public P2LConversation getOutgoingConvoFor(P2LNodeInternal parent, P2LConnection con, InetSocketAddress to, short type, short conversationId) {
        return activeOutgoingConversations.computeIfAbsent(new SenderTypeConversationIdentifier(to, type, conversationId),
                k -> new P2LConversationImplV2(parent, this, con, to, type, conversationId));
    }
    public void remove(P2LConversationImplV2 convo) {
        SenderTypeConversationIdentifier id = new SenderTypeConversationIdentifier(convo.peer, convo.type, convo.conversationId);
        //note: could also be determined over the step - because the step is set to -step when closed and if -step is odd we are a server also - but this is more readable and we don't expose step
        if(convo.isServer)  activeIncomingConversations.remove(id);
        else                activeOutgoingConversations.remove(id);
    }
}
