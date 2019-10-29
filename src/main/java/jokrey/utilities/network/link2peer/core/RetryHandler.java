package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.util.Hash;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jokrey
 */
public class RetryHandler {
    private final ConcurrentHashMap<Hash, Integer> retryCounters = new ConcurrentHashMap<>();

}
