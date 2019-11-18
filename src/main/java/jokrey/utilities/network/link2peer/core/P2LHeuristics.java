package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.P2LMessage;

/**
 * Can be changed by node operator - but always with care and knowledge(please)
 *
 * todo add explanations on why the values where chosen and what their theoretical bounds are
 */
public class P2LHeuristics {
    public static int BROADCAST_USES_HASH_DETOUR_RAW_SIZE_THRESHOLD = P2LMessage.CUSTOM_RAW_SIZE_LIMIT;
    public static long STREAM_RECEIPT_TIMEOUT_MS = 500; //todo should be variable
    /**
     * Has to be smaller 4*({@link P2LMessage#CUSTOM_RAW_SIZE_LIMIT} - ~20). This is so that the indices list in the receipt definitely fits
     * For the default value of raw limit this means that the max is: (1024-20)/4 = 251
     */
    public static int ORDERED_STREAM_CHUNK_BUFFER_ARRAY_SIZE = 2048;
    public static int ORDERED_STREAM_CHUNK_RECEIPT_RESEND_LIMITATION_IN_MS = 100;
    public static int ORIGINAL_RETRY_HISTORIC_TIMEOUT_MS = 60*1000;
    public static int DEFAULT_PROTOCOL_ATTEMPT_COUNT = 3;
    public static int DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT = 500;
    public static int DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT = DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT*2;
    public static int RETRY_HISTORIC_CONNECTION_TIMEOUT_MS = 4000;
    public static int MAIN_NODE_SLEEP_TIMEOUT_MS = 10000; //assert RETRY_HISTORIC_CONNECTION_TIMEOUT_MS < MAIN_NODE_SLEEP_TIMEOUT_MS
    public static int BROADCAST_STATE_ATTEMPT_CLEAN_KNOWN_HASH_COUNT_TRIGGER = 3500;
    public static int BROADCAST_STATE_ATTEMPT_CLEAN_TIMEOUT_TRIGGER_MS = 30*1000;
    public static int BROADCAST_STATE_KNOWN_HASH_TIMEOUT_MS = 2*60*1000;
    public static int ESTABLISHED_CONNECTION_IS_DORMANT_THRESHOLD_MS = 30*1000;
    public static long LONG_MESSAGE_RECEIVE_NO_PART_TIMEOUT_MS = 10 * 1000;

    /**
     * receiving even 1024 udp packets without any package loss is increasingly unlikely in a commodity internet setting
     *      especially with the expected associated congestion that is not considered by the VERY simple long message protocol
     * Additionally the greater this number the more effective an application layer slow loris attack on the system
     */
    public static int LONG_MESSAGE_MAX_NUMBER_OF_PARTS = 1024;
}
