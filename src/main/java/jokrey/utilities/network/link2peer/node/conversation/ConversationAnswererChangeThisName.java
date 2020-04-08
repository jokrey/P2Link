package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.network.link2peer.ReceivedP2LMessage;

import java.io.IOException;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface ConversationAnswererChangeThisName {
    void converse(P2LConversation convo, ReceivedP2LMessage m0) throws IOException;
}
