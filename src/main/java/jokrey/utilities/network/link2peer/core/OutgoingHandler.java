package jokrey.utilities.network.link2peer.core;

import jokrey.utilities.network.link2peer.util.P2LFuture;
import jokrey.utilities.network.link2peer.util.P2LThreadPool;
import jokrey.utilities.simple.data_structure.pairs.Pair;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jokrey
 */
class OutgoingHandler {
//    private final ThreadPoolExecutor sendMessagesPool = new ThreadPoolExecutor(16/*core size*/,64/*max size*/,60, TimeUnit.SECONDS/*idle timeout*/, new LinkedBlockingQueue<>(256)); // queue with a size
    private final P2LThreadPool sendMessagesPool = new P2LThreadPool(4, 64);

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
    P2LFuture<Pair<Integer, Integer>> executeAll(Task... tasks) {
        P2LFuture<Pair<Integer, Integer>> sendingDone = new P2LFuture<>();

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
                if (completedCounter.incrementAndGet() == tasks.length)
                    sendingDone.setCompleted(new Pair<>(successfullyCompletedCounter.get(), completedCounter.get()));
            });
        }
        return sendingDone;
    }

    interface Task {
        boolean execute() throws Throwable;
    }
}
