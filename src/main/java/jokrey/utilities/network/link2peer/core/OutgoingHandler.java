package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.util.P2LFuture;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jokrey
 */
class OutgoingHandler {
    private final ThreadPoolExecutor sendMessagesPool = new ThreadPoolExecutor(8/*core size*/,64/*max size*/,60, TimeUnit.SECONDS/*idle timeout*/, new LinkedBlockingQueue<>(64)); // queue with a size

    P2LFuture<Boolean> execute(Task task) {
        P2LFuture<Boolean> future = new P2LFuture<>();
        sendMessagesPool.execute(() -> {
            try {
                future.setCompleted(task.execute());
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                future.setCompleted(false);
            }
        });
        return future;
    }
    P2LFuture<Integer> executeAll(Task... tasks) {
        P2LFuture<Integer> sendingDone = new P2LFuture<>();

        AtomicInteger completedCounter = new AtomicInteger(0);
        AtomicInteger successfullyCompletedCounter = new AtomicInteger(0);

        for(Task t : tasks) {
            sendMessagesPool.execute(() -> {
                boolean successfullySend = false;
                try {
                    successfullySend = t.execute();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
                if (successfullySend)
                    successfullyCompletedCounter.getAndIncrement();
                completedCounter.getAndIncrement();
                if (completedCounter.get() == tasks.length)
                    sendingDone.setCompleted(successfullyCompletedCounter.get());
            });
        }
        return sendingDone;
    }

    interface Task {
        boolean execute() throws Throwable;
    }
}
