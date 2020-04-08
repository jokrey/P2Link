package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.network.link2peer.ReceivedP2LMessage;
import jokrey.utilities.network.link2peer.node.core.P2LConnection;
import jokrey.utilities.network.link2peer.node.core.P2LNodeInternal;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author jokrey
 */
public interface ConversationHandler {
    void registerConversationHandlerFor(int type, ConversationAnswererChangeThisName handler);
    void received(P2LNodeInternal parent, InetSocketAddress from, ReceivedP2LMessage received) throws IOException;
    void clean();
    P2LConversation getOutgoingConvoFor(P2LNodeInternal parent, P2LConnection con, InetSocketAddress to, short type, short conversationId);
    void remove(P2LConversationImplV2 convo);
}
