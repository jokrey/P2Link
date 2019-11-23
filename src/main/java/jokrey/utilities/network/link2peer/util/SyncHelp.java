package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.network.link2peer.core.stream.P2LFragmentOutputStreamImplV1;

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
    public static boolean waitUntil(Object monitor, Supplier<Boolean> condition, int timeout_ms) throws InterruptedException {
        long waitingSince = System.currentTimeMillis();
        long elapsed_ms = 0;
        synchronized (monitor) {
            while (! condition.get()) {
                monitor.wait(timeout_ms == ENDLESS_WAIT ? ENDLESS_WAIT : timeout_ms - elapsed_ms);
                elapsed_ms = System.currentTimeMillis() - waitingSince;
                if (timeout_ms != ENDLESS_WAIT && elapsed_ms >= timeout_ms) { //waiting for timeout without rechecking whether it has actually timed out is not possible - wait(timeout) is not guaranteed to sleep until timeout
                    return false;
                }
            }
        }
        return true;
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
