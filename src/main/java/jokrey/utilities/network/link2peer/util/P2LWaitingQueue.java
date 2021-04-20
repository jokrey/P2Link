package jokrey.utilities.network.link2peer.util;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class P2LWaitingQueue<I, T> {
    private final Map<I, Queue<P2LFuture<T>>> futureQueues = new ConcurrentHashMap<>();

    public final P2LFuture<T> wait(I identifier) {
        P2LFuture<T> f = new P2LFuture<>();
        futureQueues.computeIfAbsent(identifier, id -> new ConcurrentLinkedQueue<>()).add(f);
        return f;
    }

    public final boolean notify(I identifier, T result) {
        Queue<P2LFuture<T>> qAtI = futureQueues.get(identifier);
        if(qAtI == null) return false;

        P2LFuture<T> f = qAtI.poll();
        if(f == null) return false;

        if(f.isLikelyInactive()) {
            return notify(identifier, result);
        } else {
            f.setCompleted(result);
            return true;
        }
    }
}