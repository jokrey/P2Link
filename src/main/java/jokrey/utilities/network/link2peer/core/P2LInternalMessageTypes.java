package jokrey.utilities.network.link2peer.core;

public class P2LInternalMessageTypes {
    //STATE LESS - i.e. connection and therefore verified p2link does not need to be known by incoming handler to handle these:
    static final int SL_PEER_CONNECTION_REQUEST = -1;
    static final int SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -2;
    static final int SL_WHO_AM_I = -3;
    static final int SL_PING = -4;

    static final int SC_BROADCAST = -21;
    static final int SC_DISCONNECT = -22;

    static final int C_PEER_LINKS = -31;
    static final int C_BROADCAST_MSG_KNOWLEDGE_RETURN = -32;
    static final int C_BROADCAST_MSG = -33;

    static final int R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST = -991;
    static final int R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER = -992;
    static final int R_WHO_AM_I_ANSWER = -993;
    static final int R_CONNECTION_ESTABLISHED = -994;
    static final int SL_PONG = -995;

    public static boolean isInternalMessageId(int msgId) {
        return msgId < 0;
    }
    static void validateMsgIdNotInternal(int msgId) {
        if(isInternalMessageId(msgId))
            throw new IllegalArgumentException("some msg ids(for example "+msgId+") are used internally and not available at the user level, please choose a different message type");
    }
}
