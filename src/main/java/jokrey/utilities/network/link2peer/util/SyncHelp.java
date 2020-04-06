package jokrey.utilities.network.link2peer.util;

import java.io.IOException;
import java.util.function.Supplier;

import static jokrey.utilities.network.link2peer.util.P2LFuture.ENDLESS_WAIT;

/**
 * @author jokrey
 */
public class SyncHelp {
    /**
     *
     * @param monitor
     * @param condition
     * @param timeout_ms
     * @return false if a timeout occurred, true if the condition holds (note: on returned false the condition may now hold[race condition])
     * @throws InterruptedException
     */
    public static boolean waitUntil(Object monitor, Supplier<Boolean> condition, long timeout_ms) {
        if(condition.get()) return true;
        try {
            long waitingSince = System.currentTimeMillis();
            long elapsed_ms = 0;
            synchronized (monitor) {
                while (!condition.get()) {
                    monitor.wait(timeout_ms == ENDLESS_WAIT ? ENDLESS_WAIT : timeout_ms - elapsed_ms);
                    elapsed_ms = System.currentTimeMillis() - waitingSince;
                    if (timeout_ms != ENDLESS_WAIT && elapsed_ms >= timeout_ms) { //waiting for timeout without rechecking whether it has actually timed out is not possible - wait(timeout) is not guaranteed to sleep until timeout
                        return false;
                    }
                }
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
    public static boolean waitUntil(Object monitor, Supplier<Boolean> condition, long timeout_ms, long intermediateEvery, Runnable intermediateAction) {
        if(condition.get()) return true;
        if(intermediateEvery<=0) throw new IllegalArgumentException("intermediate delay cannot be endless (<=0)");
        try {
            long waitingSince = System.currentTimeMillis();
            long elapsed_ms = 0;
            synchronized (monitor) {
                boolean conditionResult = condition.get();
                while (! conditionResult) {
                    long leftToWait = timeout_ms-elapsed_ms; //never <= 0, unless timeout_ms == 0
//                    System.out.println("leftToWait = " + leftToWait);
//                    System.out.println("timeout_ms = " + timeout_ms);
//                    System.out.println("elapsed_ms = " + elapsed_ms);
//                    System.out.println("intermediateEvery = " + intermediateEvery);
                    long waitTime = timeout_ms == ENDLESS_WAIT ? intermediateEvery : Math.min(leftToWait, intermediateEvery);
//                    System.out.println("waitTime = " + waitTime);
                    monitor.wait(waitTime);
                    elapsed_ms = System.currentTimeMillis() - waitingSince;
                    if (timeout_ms != ENDLESS_WAIT && elapsed_ms >= timeout_ms)  //waiting for timeout without rechecking whether it has actually timed out is not possible - wait(timeout) is not guaranteed to sleep until timeout
                        return false;

                    conditionResult = condition.get();
                    if(conditionResult)
                        return true;
                    intermediateAction.run();
                }
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
    public static boolean waitUntilOrThrowIO(Object monitor, Supplier<Boolean> condition, long timeout_ms, IOAction intermediateAction, long intermediateEvery) throws IOException {
        if(condition.get()) return true;
        try {
            if(intermediateEvery<=0) throw new IllegalArgumentException();

            long waitingSince = System.currentTimeMillis();
            long elapsed_ms = 0;
            synchronized (monitor) {
                boolean conditionResult = condition.get();
                while (! conditionResult) {
                    monitor.wait(timeout_ms == ENDLESS_WAIT ? intermediateEvery : Math.min(timeout_ms-elapsed_ms, intermediateEvery));
                    elapsed_ms = System.currentTimeMillis() - waitingSince;
                    if (timeout_ms != ENDLESS_WAIT && elapsed_ms >= timeout_ms)  //waiting for timeout without rechecking whether it has actually timed out is not possible - wait(timeout) is not guaranteed to sleep until timeout
                        return false;

                    conditionResult = condition.get();
                    if(conditionResult)
                        return true;
                    intermediateAction.run();
                }
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    public interface IOAction {
        void run() throws IOException;
    }



    public static boolean wait(Object monitor, long timeout) {
        synchronized (monitor) {
            try {
                monitor.wait(timeout);
                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
    public static boolean wait(Object monitor) {
        synchronized (monitor) {
            try {
                monitor.wait();
                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
    public static void notify(Object monitor) {
        synchronized (monitor) {
            monitor.notify();
        }
    }
    public static void notifyAll(Object monitor) {
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }
}
