package jokrey.utilities.network.link2peer.node.core;

import jokrey.utilities.network.link2peer.P2LMessage;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface ConversationReceivalHandlerChangeThisName {
    void converse(P2LConversation convo, P2LMessage initMessage);
}
