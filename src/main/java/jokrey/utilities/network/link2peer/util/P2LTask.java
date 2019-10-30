package jokrey.utilities.network.link2peer.util;

/**
 * @author jokrey
 */
public abstract class P2LTask<R> extends P2LFuture<R> {
    private boolean started = false;
    public void start() {
        started = true;
        try {
            setCompletedOrCanceledBasedOn(run());
        } catch (Throwable t) {
            t.printStackTrace();
            cancel();
        }
    }
    public void startOnThread() {
        new Thread(this::start).start();
    }
    public boolean isRunning() {
        return started && !isCanceled() && !isCompleted();
    }
    protected abstract R run();
}