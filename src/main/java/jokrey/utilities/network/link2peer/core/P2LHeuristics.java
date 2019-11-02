package jokrey.utilities.network.link2peer.core;

/**
 * Can be changed by node operator - but always with care and knowledge(please)
 */
public class P2LHeuristics {
    public static int ORIGINAL_RETRY_HISTORIC_TIMEOUT_MS = 60*1000;
    public static int DEFAULT_PROTOCOL_ATTEMPT_COUNT = 3;
    public static int DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT = 500;
    public static int DEFAULT_PROTOCOL_ANSWER_RECEIVE_TIMEOUT = DEFAULT_PROTOCOL_ATTEMPT_INITIAL_TIMEOUT*2;
    public static int RETRY_HISTORIC_CONNECTION_TIMEOUT_MS = 4000;
    public static int MAIN_NODE_SLEEP_TIMEOUT_MS = 10000; //assert RETRY_HISTORIC_CONNECTION_TIMEOUT_MS < MAIN_NODE_SLEEP_TIMEOUT_MS
    public static int BROADCAST_STATE_ATTEMPT_CLEAN_KNOWN_HASH_COUNT_TRIGGER = 3500;
    public static int BROADCAST_STATE_ATTEMPT_CLEAN_TIMEOUT_TRIGGER_MS = 30*1000;
    public static int BROADCAST_STATE_KNOWN_HASH_TIMEOUT_MS = 2*60*1000;
    public static int ESTABLISHED_CONNECTION_IS_DORMANT_THRESHOLD_MS = 2*60*1000;
    public static long LONG_MESSAGE_RECEIVE_NO_PART_TIMEOUT_MS = 10 * 1000;
    public static int LONG_MESSAGE_MAX_NUMBER_OF_PARTS = 512; //receiving more than 512 udp packets without any package loss is increasingly unlikely in a commodity internet setting
}
