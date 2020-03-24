package jokrey.utilities.network.link2peer.util;

import java.util.function.Consumer;

/**
 * The given attempt function has to register a callback to call resultFunc itself.
 *
 * @author jokrey
 */
public class AsyncLoop<T> extends P2LFuture<Boolean> implements Consumer<T>  {
    private final LoopStep<T> loopFunc;

    public AsyncLoop(LoopStep<T> loopFunc) {
        this.loopFunc = loopFunc;
        start();
    }

    @Override public void accept(T toHandle) {
        try {
            P2LFuture<T> fut = loopFunc.handle(toHandle);
            if(fut != null) {
                fut.whenCompleted(this);
                fut.whenCanceled(this::cancelIfNotCompleted);
            } else {
                setCompleted(true);
            }
        } catch (Throwable e) {
            fail(e);
        }
    }

    private void fail(Throwable e) {
        e.printStackTrace();
        //todo - how do we handle? - is this sufficient?!:
        cancel();
    }

    private void start() {
        accept(null);
    }

    public interface LoopStep<T> {
        /**@return whether the result was accepted */
        P2LFuture<T> handle(T result) throws Throwable;
    }
    public interface FutureStep<T> {
        /**@return whether the result was accepted */
        P2LFuture<T> next();
    }
}
