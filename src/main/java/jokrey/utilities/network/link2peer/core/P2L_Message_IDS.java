package jokrey.utilities.network.link2peer.core;

public class P2L_Message_IDS {
    public static final int IC_CONNECTION_REQUEST_LINK_VERIFY = -1;
    public static final int IC_NEW_PEER_CONNECTION = -2;
    public static final int IC_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -3;

    public static final int SC_BROADCAST = -1;
    public static final int SC_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -2;

    public static final int C_PEER_LINKS = -11;
    public static final int C_BROADCAST_HASH = -12;
    public static final int C_BROADCAST_MSG_KNOWLEDGE_RETURN = -13;
    public static final int C_BROADCAST_MSG = -14;

    public static final int R_CONNECTION_DENIED_TOO_MANY_PEERS = -1;
    public static final int R_CONNECTION_ACCEPTED = -2;
    public static final int R_LINK_VALID = -3;
    public static final int DEFAULT_ID = 0;

    public static boolean isInternalMessageId(int msgId) {
        return msgId < 0;
    }
    public static void validateMsgIdNotInternal(int msgId) {
        if(isInternalMessageId(msgId))
            throw new IllegalArgumentException("some msg ids(for example "+msgId+") are used internally and not available at the user level, please choose a different message type");
    }
}
