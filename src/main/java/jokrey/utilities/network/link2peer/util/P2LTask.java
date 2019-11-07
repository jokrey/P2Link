package jokrey.utilities.network.link2peer.util;

/**
 * Task that can be run by the {@link P2LThreadPool}.
 * Additionally it is a future, i.e. the underlying task produces a result that completes said future.
 * If the underlying task throws an exception, the future is canceled.
 *
 * @author jokrey
 */
public abstract class P2LTask<R> extends P2LFuture<R> {
    private boolean started = false;
    /**
     * Runs the operation of this task. It produces an arbitrary result that completes the underlying future.
     * If it throws an exception this tasks underlying future is canceled.
     */
    void start() {
        started = true;
        try {
            R resultOrNull = run();
            if (resultOrNull == null) cancel();
            else if(!isCanceled()) super.setCompleted(resultOrNull);
        } catch (Throwable t) {
            t.printStackTrace();
            cancel(); //cannot be completed at this point
        }
    }

    /** UNSUPPORTED FOR TASK FROM OUTSIDER - canceling a task on the other hand is fine, that can be alternatively handled by the run method of the task */
    @Override public void setCompleted(R result) {
        throw new UnsupportedOperationException("cannot set a task to complete from outside the task itself");
    }

    /**
     * Starts the task on a new thread.
     * When the task completes it sets this future to complete.
     */
    public void startOnThread() {
        new Thread(this::start).start();
    }

    /**
     * @return whether this task is still running
     */
    public boolean isRunning() {
        return started && !isCanceled() && !isCompleted();
    }

    /**
     * Abstraction of the operation run by this task. It produces an arbitrary result that completes the underlying future.
     * If it throws an exception this tasks underlying future is canceled.
     * @return the value created by this task
     */
    protected abstract R run();
}