package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.simple.data_structure.pairs.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * @author jokrey
 */
public class AsyncTimeoutSchedulerThread {
    private static AsyncTimeoutSchedulerThread instance = null;
    public static AsyncTimeoutSchedulerThread instance() {
        if(instance == null) {
            synchronized (AsyncTimeoutSchedulerThread.class) {
                if(instance == null){
                    instance = new AsyncTimeoutSchedulerThread();
                    return instance;
                }
            }
        }
        return instance;
    }
    public static void shutdown() {
        synchronized (AsyncTimeoutSchedulerThread.class) {
            if(instance != null) {
                instance.shutdownAsap();
                instance = null;
            }
        }
    }

    private void shutdownAsap() {
        synchronized (this) {
            shutdown = true;
            instance.notify();
        }
    }

    private volatile boolean shutdown = false;
    private final TreeMap<Long, P2LFuture<?>> scheduledTimeouts = new TreeMap<>();

    private AsyncTimeoutSchedulerThread() {
        new Thread(() -> {
            while (!shutdown) {
                P2LFuture<?> futureToCancel;
                synchronized (AsyncTimeoutSchedulerThread.this) {
                    while (!shutdown && (scheduledTimeouts.isEmpty() || scheduledTimeouts.firstKey() > System.currentTimeMillis())) {
                        try {
                            if (scheduledTimeouts.isEmpty())
                                wait();
                            else
                                wait(scheduledTimeouts.firstKey() - System.currentTimeMillis());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    futureToCancel = scheduledTimeouts.remove(scheduledTimeouts.firstKey());
                }
                if(shutdown) break;
                //PROBLEM: If a scheduled cancellation has callbacks that attempts to schedule further timeouts, this entire thread deadlocks.......
                //         Which is why this fine grained locking is required..
                futureToCancel.cancelIfNotCompleted();
            }
        }).start();
    }

    public void add(int timeout, P2LFuture<?> future) {
        synchronized(this) {
            long timeoutAt = System.currentTimeMillis() + timeout;
            scheduledTimeouts.put(timeoutAt, future);
            instance.notify();
        }
    }
}
