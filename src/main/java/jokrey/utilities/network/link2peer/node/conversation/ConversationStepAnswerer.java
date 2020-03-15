package jokrey.utilities.network.link2peer.node.conversation;

import jokrey.utilities.network.link2peer.P2LMessage;

import java.io.IOException;

/**
 * @author jokrey
 */
@FunctionalInterface
public interface ConversationStepAnswerer {
    void step(P2LMessage mx, boolean timeout) throws IOException;
}
