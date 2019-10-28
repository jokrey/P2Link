package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.util.Hash;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author jokrey
 */
public class RetryHandler {
    private final ConcurrentHashMap<Hash, Integer> retryCounters = new ConcurrentHashMap<>();

}
