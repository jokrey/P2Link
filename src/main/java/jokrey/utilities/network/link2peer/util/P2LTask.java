package jokrey.utilities.network.link2peer.util;

/**
 * @author jokrey
 */
public abstract class P2LTask<R> extends P2LFuture<R> {
    private boolean started = false;
    public void start() {
        started = true;
        setCompleted(run());
    }
    public void startOnThread() {
        new Thread(this::start).start();
    }
    public boolean isRunning() {
        return started && !isCanceled() && !isCompleted();
    }
    protected abstract R run();
}