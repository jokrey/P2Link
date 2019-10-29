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
    private Queue<Runnable> queuedTasks = new LinkedList<>();

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
        for(P2LThread t:pool) t.cancel();
        pool.clear();
        queuedTasks.clear();
    }
    private synchronized void taskFinished(P2LThread noLongerOccupied) {
        if(shutdown) return;

        Runnable unstartedTask = queuedTasks.poll();
        if(unstartedTask==null) {
            synchronized (this) {
                if (pool.size() > coreThreads)
                    pool.remove(noLongerOccupied);
            }
        } else
            noLongerOccupied.runTask(unstartedTask);
    }

    public synchronized void execute(Runnable r) {
        if(shutdown) return;

        int size = pool.size();
        Iterator<P2LThread> poolIterator = pool.iterator();
        while (poolIterator.hasNext()) {
            P2LThread thread = poolIterator.next();
            if (r != null && thread.runTask(r)) {
                r = null;
//                    System.out.println("running on existing");
            } else if (size > coreThreads && thread.cancel()) {
                poolIterator.remove();
                size--;
//                    System.out.println("clean one");
            }
        }
        if(r!=null) {
            if(size<maxThreads) {
//                    System.out.println("creating thread");
                pool.add(new P2LThread(r));
            } else if(queuedTasks.size() < maxQueuedTasks) {
//                    System.out.println("enqueuing");
                queuedTasks.offer(r);
            } else {
                throw new CapacityReachedException();
            }
        }
//        System.out.println("queuedTasks.size = " + queuedTasks.size());
//        System.out.println("pool.size() = " + pool.size());
    }

    class P2LThread implements Runnable {
        boolean canceled = false;
        private Runnable task;
        P2LThread() { this(null); }
        P2LThread(Runnable task) {
            new Thread(this).start();
            runTask(task);
        }

        synchronized boolean cancel() {
            if(task!=null) return false;
            canceled = true;
            notify();
            return true;
        }

        synchronized boolean runTask(Runnable r) {
            if(task == null) {
                task = r;
                notify();
                return true;
            }
            return false;
        }

        @Override public void run() {
            while(!canceled) {
                if(task!=null) {
                    try {
                        task.run();
                    } catch (Throwable t) {t.printStackTrace();}
                    task = null;
                    taskFinished(this);
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
}
