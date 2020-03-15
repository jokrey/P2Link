package jokrey.utilities.network.link2peer.node.conversation;

import java.io.IOException;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface ConversationCloseCallback {
    void finalize(boolean success) throws IOException;
}
