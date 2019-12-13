package jokrey.utilities.network.link2peer.node.core;

//ALL IDS SHOULD BE UNIQUE (they don't all need to be, but they should be(there are enough...)
public class P2LInternalMessageTypes {
    //STATE LESS - i.e. connection and therefore verified p2link does not need to be known by incoming handler to handle these:
    public static final short SL_DIRECT_CONNECTION_REQUEST = -1;
    public static final short SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -2;
    public static short SL_WHO_AM_I = -3;
    public static short SL_PING = -4;
    public static short SL_REQUEST_DIRECT_CONNECT_TO = -5;
    public static short SL_RELAY_REQUEST_DIRECT_CONNECT = -6;

    public static short SC_BROADCAST_WITH_HASH = -21;
    public static short SC_DISCONNECT = -22;
    public static short SC_BROADCAST_WITHOUT_HASH = -23;

    public static short C_PEER_LINKS = -31;
    public static short C_BROADCAST_HASH_KNOWLEDGE_ANSWER = -32;
    public static short C_BROADCAST_MSG = -33;

    public static short R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_REQUEST = -991;
    public static short R_DIRECT_CONNECTION_REQUEST_VERIFY_NONCE_ANSWER = -992;
    public static short R_WHO_AM_I_ANSWER = -993;
    public static short R_DIRECT_CONNECTION_ESTABLISHED = -994;

    public static boolean isInternalMessageId(int msgId) {
        return msgId < 0;
    }
    public static void validateMsgTypeNotInternal(int msgId) {
        if(isInternalMessageId(msgId))
            throw new IllegalArgumentException("some msg ids(for example "+msgId+") are used internally and not available at the user level, please choose a different message type");
    }
}