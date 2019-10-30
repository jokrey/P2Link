package jokrey.utilities.network.link2peer.util;

import java.util.*;

/**
 * todo WAY TOO MUCH SYNCHRONIZED (everything is locked
 *
 * @author jokrey
 */
public class P2LThreadPool {
    private final int coreThreads, maxThreads, maxQueuedTasks;
    private List<P2LThread> pool;
    private Queue<P2LTask> queuedTasks = new LinkedList<>();

    public P2LThreadPool(int coreThreads, int maxThreads) {
        this(coreThreads, maxThreads, Integer.MAX_VALUE);
    }
    public P2LThreadPool(int coreThreads, int maxThreads, int maxQueuedTasks) {
        this.coreThreads = coreThreads;
        this.maxThreads = maxThreads;
        this.maxQueuedTasks = maxQueuedTasks;
        pool = new ArrayList<>(maxThreads);

        for(int i=0;i<coreThreads;i++)
            pool.add(new P2LThread());
    }

    private boolean shutdown = false;
    public synchronized void shutdown() {
        shutdown = true;
        for(P2LThread t:pool) t.shutdown();
        pool.clear();
        queuedTasks.clear();
    }
    private synchronized void taskFinished(P2LThread noLongerOccupied) {
        if(shutdown) return;

        P2LTask unstartedTask = queuedTasks.poll();
        if(unstartedTask==null) {
            synchronized (this) {
                if (pool.size() > coreThreads)
                    pool.remove(noLongerOccupied);
            }
        } else
            noLongerOccupied.runTask(unstartedTask);
    }

    public synchronized P2LFuture<Integer> execute(Task... tasks) {
        ArrayList<P2LFuture<Boolean>> futures = new ArrayList<>(tasks.length);
        for(Task task:tasks)
            futures.add(execute(task));
        return P2LFuture.reduceConvertWhenCompleted(futures, b -> b?1:0, P2LFuture.PLUS);
    }
    public synchronized P2LTask<Boolean> execute(Task task) {
        return execute(() -> {
            try {
                task.run();
                return true;
            } catch (Throwable t) {
                t.printStackTrace();
                return false;
            }
        });
    }
    public synchronized <R>P2LTask<R> execute(ProvidingTask<R> task) {
        return execute(new P2LTask<R>() {
            @Override protected R run() {
                try {
                    return task.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                    return null;//sets task to canceled
                }
            }
        });
    }
    public synchronized <R>P2LTask<R> execute(P2LTask<R> task) {
        if(task == null) throw new NullPointerException();
        if(shutdown) throw new ShutDownException();

        int size = pool.size();
        boolean isCommitted = false;
        Iterator<P2LThread> poolIterator = pool.iterator();
        while (poolIterator.hasNext()) {
            P2LThread thread = poolIterator.next();
            if (!isCommitted && thread.runTask(task)) {
                isCommitted = true;
            } else if (size > coreThreads && thread.shutdown()) {
                poolIterator.remove();
                size--;
            }
        }
        if(!isCommitted) {
            if(size<maxThreads) {
                pool.add(new P2LThread(task));
            } else if(queuedTasks.size() < maxQueuedTasks) {
                queuedTasks.offer(task);
            } else {
                throw new CapacityReachedException();
            }
        }

        return task;
    }

    private class P2LThread implements Runnable {
        boolean shutdown = false;
        private P2LTask task;
        P2LThread() { this(null); }
        P2LThread(P2LTask task) {
            new Thread(this).start();
            runTask(task);
        }

        synchronized boolean shutdown() {
            if(task!=null) return false;
            shutdown = true;
            notify();
            return true;
        }

        synchronized boolean runTask(P2LTask t) {
            if(task == null) {
                task = t;
                notify();
                return true;
            }
            return false;
        }

        @Override public void run() {
            while(!shutdown) {
                if(task!=null) {
                    task.start();
                    task = null;
                    P2LThreadPool.this.taskFinished(this);
                } else {
                    synchronized (this) {
                        try {
                            wait();
                        } catch (InterruptedException e) { e.printStackTrace(); }
                    }
                }
            }
        }
    }

    @FunctionalInterface
    public interface Task {
        void run() throws Throwable;
    }
    @FunctionalInterface
    public interface ProvidingTask<R> {
        R run() throws Throwable;
    }
}