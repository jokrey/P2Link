package jokrey.utilities.network.link2peer.util;

import java.util.TreeMap;

/**
 * TODO - this thread might become 'crowded' and drop in performance if the scheduled runnables take too long.
 *
 * @author jokrey
 */
public class AsyncCallbackSchedulerThread {
    private static AsyncCallbackSchedulerThread instance = null;
    public static AsyncCallbackSchedulerThread instance() {
        if(instance == null) {
            synchronized (AsyncCallbackSchedulerThread.class) {
                if(instance == null){
                    instance = new AsyncCallbackSchedulerThread();
                    return instance;
                }
            }
        }
        return instance;
    }
    public static void shutdown() {
        synchronized (AsyncCallbackSchedulerThread.class) {
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
    private final TreeMap<Long, Runnable> scheduledTimeouts = new TreeMap<>();

    private AsyncCallbackSchedulerThread() {
        new Thread(() -> {
            while (!shutdown) {
                Runnable runnableToRun;
                synchronized (AsyncCallbackSchedulerThread.this) {
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
                    runnableToRun = scheduledTimeouts.remove(scheduledTimeouts.firstKey());
                }
                if(shutdown) break;
                //PROBLEM: If a scheduled cancellation has callbacks that attempts to schedule further timeouts, this entire thread deadlocks.......
                //         Which is why this fine grained locking is required..
                runnableToRun.run();
            }
        }).start();
    }

    public void add(int timeout, Runnable runnableToRun) {
        synchronized(this) {
            long timeoutAt = System.currentTimeMillis() + timeout;
            scheduledTimeouts.put(timeoutAt, runnableToRun);
            instance.notify();
        }
    }
    public void add(int timeout, P2LFuture<Boolean> futureToComplete) {
        synchronized(this) {
            long timeoutAt = System.currentTimeMillis() + timeout;
            scheduledTimeouts.put(timeoutAt, () -> futureToComplete.setCompleted(true));
            instance.notify();
        }
    }
    public P2LFuture<Boolean> completeAfter(int timeout) {
        if(timeout <= 0) return new P2LFuture<>(true);
        P2LFuture<Boolean> futureToComplete = new P2LFuture<>();
        add(timeout, futureToComplete);
        return futureToComplete;
    }

    public <T>P2LFuture<T> runAfter(int timeout, AsyncLoop.FutureStep<T> toRun) {
        if(timeout <= 0) return toRun.next();
        P2LFuture<T> futureToComplete = new P2LFuture<>();
        add(timeout, () -> {
            toRun.next().callMeBack(futureToComplete::setCompletedOrCanceledBasedOn);
        });
        return futureToComplete;
    }
}
