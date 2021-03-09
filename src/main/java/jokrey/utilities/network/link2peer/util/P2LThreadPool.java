package jokrey.utilities.network.link2peer.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * todo WAY TOO MUCH SYNCHRONIZED (everything is locked)
 *
 *
 * TODO : cool would be to allow tasks waiting on futures to be re-added to queued tasks if other tasks are waiting... Go-style green threads
 *      todo - that would be really cool, but it is not possible, right? Because we would need to store the stack and exec pointer (which is not possible in plain java, right?)
 *
 * @author jokrey
 */
public class P2LThreadPool {
    private final int coreThreads, maxThreads, maxQueuedTasks;
    private final List<P2LThread> pool;
    private final Queue<P2LTask<?>> queuedTasks = new LinkedList<>();

    public P2LThreadPool(int coreThreads, int maxThreads) {
        this(coreThreads, maxThreads, Integer.MAX_VALUE);
    }
    public P2LThreadPool(int coreThreads, int maxThreads, int maxQueuedTasks) {
        if(maxThreads < coreThreads) throw new IllegalArgumentException("cannot have more core threads("+coreThreads+") than max threads("+maxThreads+")");
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
    private void taskFinished(P2LThread noLongerOccupied) {
        if(shutdown) return;

        P2LTask<?> unstartedTask;
        synchronized (this) {
            unstartedTask = queuedTasks.poll();
//            System.out.println("taskFinished - left="+queuedTasks.size()+" - new="+unstartedTask);
            if (unstartedTask==null && pool.size() > coreThreads && noLongerOccupied.shutdownIfIdle()) {
                pool.remove(noLongerOccupied);
//                System.out.println("thread removed");
            }
        }
        if(unstartedTask != null) {
            boolean taskScheduledSuccessfully = noLongerOccupied.runTask(unstartedTask);
            if(!taskScheduledSuccessfully) {
                synchronized (this) {
                    queuedTasks.offer(unstartedTask);
//                    System.out.println("reoffered");
                }
            }
//            else
//                System.out.println("run on previously occupied");
        }
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
//                System.out.println("execute.runOnExisting");
                isCommitted = true;
            } else if (size > coreThreads && thread.shutdownIfIdle()) {
//                System.out.println("thread removed");
                poolIterator.remove();
                size--;
            }
        }
        if(!isCommitted) {
            if(size<maxThreads) {
//                System.out.println("execute.runOnNew");
                pool.add(new P2LThread(task));
//                System.out.println("thread added");
            } else if(queuedTasks.size() < maxQueuedTasks) {
//                System.out.println("execute.offered");
                queuedTasks.offer(task);
            } else {
                throw new CapacityReachedException();
            }
        }

        return task;
    }

    private class P2LThread implements Runnable {
        boolean shutdown = false;
        private final AtomicReference<P2LTask<?>> task = new AtomicReference<>(null);
        P2LThread() { this(null); }
        P2LThread(P2LTask<?> task) {
            runTask(task);
            new Thread(this).start();
        }

        synchronized void shutdown() {
            shutdown = true;
            P2LTask<?> before = task.getAndSet(null);
            if(before!=null)
                before.tryCancel();
            synchronized (task) {task.notifyAll();}
        }
        synchronized boolean shutdownIfIdle() {
            if(task.get() != null) return false;
            //if a new task is added here, but not yet executed and then this thread is shutdown - then that task is lost and never executed...
            shutdown();
            return true;
        }
        synchronized boolean runTask(P2LTask<?> t) {
            if(!shutdown && t!=null && task.compareAndSet(null, t)) {
                synchronized (task) {task.notifyAll();}
//                System.out.println("runTask("+t+") - SUCCESS");
                return true;
            }
//            System.out.println("runTask() - FAIL");
            return false;
        }

        @Override public void run() {
            while(!shutdown) {
                P2LTask<?> t = task.get();
                if(t!=null) {
                    t.start();
                    task.set(null);
                    //here 'runTask' can swoop in and take the thread - so task finished has to handle that scenario
                    P2LThreadPool.this.taskFinished(this);
                } else {
                    synchronized (task) {
                        try {
                            task.wait();
                        } catch (InterruptedException e) { e.printStackTrace(); }

//                        P2LThreadPool.this.taskFinished(this);

//                        System.out.println("P2LThreadPool.this = " + P2LThreadPool.this.debugString());
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


    public String debugString() {
        return "P2LThreadPool{" +
                "coreThreads=" + coreThreads +
                ", maxThreads=" + maxThreads +
                ", maxQueuedTasks=" + maxQueuedTasks +
                ", pool.size=" + pool.size() +
                ", queuedTasks.size=" + queuedTasks.size() +
                ", isShutdown?=" + shutdown +
                '}';
    }











    public synchronized P2LFuture<Integer> execute(Task... tasks) {
        ArrayList<P2LFuture<Boolean>> futures = new ArrayList<>(tasks.length);
        for(Task task:tasks)
            futures.add(execute(task));
        return P2LFuture.reduceConvertWhenCompleted(futures, b -> b?1:0, P2LFuture.PLUS);
    }
    public synchronized P2LFuture<Integer> executeThreadedSuccessCounter(List<Task> tasks) {
        ArrayList<P2LFuture<Boolean>> futures = new ArrayList<>(tasks.size());
        for(Task task:tasks)
            futures.add(execute(task));
        return P2LFuture.reduceConvertWhenCompleted(futures, b -> b?1:0, P2LFuture.PLUS);
    }
    public synchronized P2LFuture<Integer> executeThreadedCounter(ProvidingTask<Boolean>... tasks) {
        ArrayList<P2LFuture<Boolean>> futures = new ArrayList<>(tasks.length);
        for(ProvidingTask<Boolean> task:tasks)
            futures.add(execute(task));
        return P2LFuture.reduceConvertWhenCompleted(futures, b -> b?1:0, P2LFuture.PLUS);
    }
    public synchronized P2LFuture<Integer> executeThreadedCounter(List<ProvidingTask<Boolean>> tasks) {
        if(tasks.isEmpty())
            return new P2LFuture<>(new Integer(0));
        ArrayList<P2LFuture<Boolean>> futures = new ArrayList<>(tasks.size());
        for(ProvidingTask<Boolean> task:tasks)
            futures.add(execute(task));
        return P2LFuture.reduceConvertWhenCompleted(futures, b -> b?1:0, P2LFuture.PLUS);
    }
    public synchronized P2LTask<Boolean> execute(Task task) {
        return execute(() -> {
            try {
                task.run();
                return true;
            } catch (ShutDownException t) {
                return false;
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
                } catch (ShutDownException t) {
                    return null;//sets task to canceled
                } catch (Throwable t) {
                    t.printStackTrace();
                    return null;//sets task to canceled
                }
            }
        });
    }
    public synchronized <R> List<P2LTask<R>> execute(ProvidingTask<R>... tasks) {
        ArrayList<P2LTask<R>> futures = new ArrayList<>(tasks.length);
        for(ProvidingTask<R> task : tasks)
            futures.add(execute(task));
        return futures;
    }
    public synchronized <R> List<P2LTask<R>> execute(List<ProvidingTask<R>> tasks) {
        ArrayList<P2LTask<R>> futures = new ArrayList<>(tasks.size());
        for(ProvidingTask<R> task : tasks)
            futures.add(execute(task));
        return futures;
    }

    public static P2LTask<Boolean> executeSingle(Task t) {
        P2LTask<Boolean> task = new P2LTask<Boolean>() {
            @Override protected Boolean run() {
                try {
                    t.run();
                    return true;
                } catch (Throwable t) {
                    t.printStackTrace();
                    return false;//sets task to canceled
                }
            }
        };
        new Thread(task::start).start();
        return task;
    }

    public static <R>P2LTask<R> executeSingle(ProvidingTask<R> t) {
        P2LTask<R> task = new P2LTask<R>() {
            @Override protected R run() {
                try {
                    return t.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                    return null;//sets task to canceled
                }
            }
        };
        new Thread(task::start).start();
        return task;
    }

}