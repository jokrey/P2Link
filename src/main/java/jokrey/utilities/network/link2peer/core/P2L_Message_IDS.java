package jokrey.utilities.network.link2peer.core;

public class P2L_Message_IDS {
    //STATE LESS - i.e. connection and therefore verified p2link does not need to be known by incoming handler to handle these:
    public static final int SL_LINK_VERIFY_REQUEST = -1;
    public static final int SL_LINK_VERIFY_ANSWER = -2;
    public static final int SL_PEER_CONNECTION_REQUEST = -3;
    public static final int SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -4;

    public static final int SC_BROADCAST = -21;
    public static final int SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -22;

    public static final int C_PEER_LINKS = -31;
    public static final int C_BROADCAST_HASH = -32;
    public static final int C_BROADCAST_MSG_KNOWLEDGE_RETURN = -33;
    public static final int C_BROADCAST_MSG = -34;

    public static final int R_CONNECTION_DENIED_TOO_MANY_PEERS = -991;
    public static final int R_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST = -992;
    public static final int R_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER = -993;
    public static final int R_LINK_VALID = -994;

    public static boolean isInternalMessageId(int msgId) {
        return msgId < 0;
    }
    public static void validateMsgIdNotInternal(int msgId) {
        if(isInternalMessageId(msgId))
            throw new IllegalArgumentException("some msg ids(for example "+msgId+") are used internally and not available at the user level, please choose a different message type");
    }
}
