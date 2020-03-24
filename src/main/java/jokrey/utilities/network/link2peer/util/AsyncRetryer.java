package jokrey.utilities.network.link2peer.util;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * The given attempt function has to register a callback to call resultFunc itself.
 *
 * @author jokrey
 */
public class AsyncRetryer<T, E> implements Consumer<E> {
    public final P2LFuture<T> finalResultFuture = new P2LFuture<>();

    protected int counter = 0;
    protected final int maxAttempts;
    public ResultHandler<T, E> resultFunc;
    public Runnable finalResultCanceledHandler;
    public RunnableThrowingThrowable attemptFunction;
    public AsyncRetryer(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        finalResultFuture.callMeBack(t -> {
            if(t == null && finalResultCanceledHandler!=null) //canceled
                finalResultCanceledHandler.run();
        });
    }

    public boolean stopButDoNotComplete = false;

    @Override public void accept(E toHandle) {
        try {
            T result = resultFunc.handle(toHandle);
            if(stopButDoNotComplete) return; //used when the result is available, but the final result future shall not be set - it will be set by the owner of this retryer itself.

            if(result == null && !finalResultFuture.isLikelyInactive()) {
                if (counter < maxAttempts) {
                    makeAttempt();
                } else {
                    finalResultFuture.cancel();
                }

                counter++;
            } else if(result != null) {
                finalResultFuture.setCompleted(result);
            }

        } catch (Throwable e) {
            fail(e);
        }
    }

    private void makeAttempt() {
        try {
            attemptFunction.run();
        } catch (Throwable e) {
            fail(e);
        }
    }

    private void fail(Throwable e) {
        e.printStackTrace();
        //todo - how do we handle? - is this sufficient?!:
        finalResultFuture.cancel();
    }

    public void start() {
        if(counter!=0) throw new IllegalStateException("cannot start twice");
        makeAttempt();
    }

    public interface ResultHandler<T, E> {
        /**@return whether the result was accepted */
        T handle(E result) throws IOException;
    }
    public interface RunnableThrowingThrowable {
        /**@return whether the result was accepted */
        void run() throws Throwable;
    }
}
