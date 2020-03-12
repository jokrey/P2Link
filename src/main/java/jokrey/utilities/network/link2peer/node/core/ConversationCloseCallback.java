package jokrey.utilities.network.link2peer.node.core;

import java.io.IOException;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface ConversationCloseCallback {
    void finalize(boolean success) throws IOException;
}
