package jokrey.utilities.network.link2peer.node.core;

//ALL IDS SHOULD BE UNIQUE (they don't all need to be, but they should be(there are enough...)
public class P2LInternalMessageTypes {
    //STATE LESS - i.e. connection and therefore verified p2link does not need to be known by incoming handler to handle these:
    public static final short SL_DIRECT_CONNECTION_REQUEST = -1;
    public static final short SL_REQUEST_KNOWN_ACTIVE_PEER_LINKS = -2;
    public static final short SL_WHO_AM_I = -3;
    public static final short SL_PING = -4;
    public static final short SL_REQUEST_DIRECT_CONNECT_TO = -5;
    public static final short SL_CONNECTION_RELAY = -6;
    public static final short SL_NAT_HOLE_PACKET = -7;
    public static final short SL_REQUEST_DIRECT_CONNECT_TO_MODIFY_DESTINATION_IP_TO_RELAY_IP = -8;

    public static final short RENDEZVOUS_C_REGISTER = -10;
    public static final short RENDEZVOUS_C_REQUEST = -11;
    public static final short RENDEZVOUS_C_REQUEST_ALL = -12;

    public static final short SC_BROADCAST_WITH_HASH = -21;
    public static final short SC_DISCONNECT = -22;
    public static final short SC_BROADCAST_WITHOUT_HASH = -23;


    public static boolean isInternalMessageId(int msgId) {
        return msgId < 0;
    }
    public static void validateMsgTypeNotInternal(int msgId) {
        if(isInternalMessageId(msgId))
            throw new IllegalArgumentException("some msg ids(for example "+msgId+") are used internally and not available at the user level, please choose a different message type");
    }
}
