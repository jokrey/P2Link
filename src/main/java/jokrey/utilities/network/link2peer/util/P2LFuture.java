package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.simple.data_structure.stack.LFStack;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Generic implementation of the 'future' concept.
 * Used to safely share an object between threads.
 *
 * Multiple threads can wait for a result. Either in a blocking fashion, with an optional timeout, using {@link #get()},
 *   or with a callback using {@link #callMeBack(Consumer)}.
 * Only one thread can set the result, if a result is set it cannot be altered.
 *
 * This class is thread safe in respect to its methods.
 *
 *
 * TODO: make cancelable
 *
 * @author jokrey
 */
public class P2LFuture<T> {
    public static final int ENDLESS_WAIT = 0;
    public static final int DEFAULT_TIMEOUT = ENDLESS_WAIT;

    private final long defaultTimeoutMs; //0 is endless wait...
    /**
     * Creates a new, not yet future.
     *
     * The default timeout of its blocking methods is set to 'endless wait'.
     */
    public P2LFuture() {this(DEFAULT_TIMEOUT);}
    /**
     * Creates a new, not yet completed future with the given default timeout in milliseconds.
     * This timeout is used by both {@link #get()} and {@link #getOrNull()}.
     *
     * Note that this is not a guarantee the timeout is used, since the timeout can be overridden by the blocking wait methods.
     *
     * @param defaultTimeoutMs the default timeout in milliseconds - has to be greater or equal to 0, otherwise an IllegalArgumentException is thrown
     */
    public P2LFuture(long defaultTimeoutMs) {
        this.defaultTimeoutMs = defaultTimeoutMs;
        if(defaultTimeoutMs<0)
            throw new IllegalArgumentException("timeout has to be >= 0");
    }
    /**
     * Creates a new, directly completed future.
     * When {@link #get()} or {@link #callMeBack(Consumer)} are called on this method, the will instantly present the result.
     *
     * Useful when the future type is required, but the result is already available.
     *
     * @param result the instantly available result.
     */
    public P2LFuture(T result) {
        this(DEFAULT_TIMEOUT);
        this.result.set(result);
    }

    private volatile boolean isGetWaiting= false;
    private volatile boolean isCanceled = false;
    private volatile boolean hasTimedOut = false;
    private final LFStack<Consumer<T>> resultCallbacks = new LFStack<>();
    private final AtomicReference<T> result = new AtomicReference<>(null);

    /** @return whether anyone is waiting for the future in a blocking fashion */
    public boolean isBlockingWaiting() { return isGetWaiting; }
    /** @return whether anyone is waiting for the future in an unblocking fashion */
    public boolean isUnblockingWaiting() {
        return resultCallbacks.size()>0;
    }
    /** @return whether anyone is waiting for the future */
    public boolean isWaiting() { return !isCanceled && (isBlockingWaiting() || isUnblockingWaiting()); }
    /** @return whether the result is available */
    public boolean isCompleted() { return result.get() != null; }
    /** @return whether the result will ever be available */
    public boolean isCanceled() { return isCanceled; }
    /** @return whether the result will ever be available */
    public boolean hasTimedOut() { return hasTimedOut; }
    /** @return the result or null if it is not yet available */
    public T getResult() { return result.get(); }

    /**
     * If the result is already available, the callback will be called on the current thread immediately - before returning to the caller
     * If the result is not yet available, the callback will be called at any point in the future from the thread that sets the result as completed.
     *
     * The order in which callbacks will be called is not strictly defined and may be subject to change. It should not be relied upon.
     *
     * @param callback callback receiving the result
     */
    public void callMeBack(Consumer<T> callback) {
        if(isCompleted())
            callback.accept(getResult());
        else
            resultCallbacks.push(callback);
    }

    /**
     * Waits for the result.
     *
     * Times out with a TimeoutException, after the default number of ms set in the constructor
     *   if no value was given in the constructor, get waits indefinitely until the result is available
     *
     * @return the result, once it is available
     * @throws TimeoutException if the result is not available after the default timeout set in the constructor
     */
    public T get() {
        return get(defaultTimeoutMs);
    }

    /**
     * Waits for the result.
     *
     * Times out with a TimeoutException, after the number of milliseconds given
     *   the given value has to be greater or equal to 0.
     *   if the value is 0, this method will wait indefinitely until a result is available.
     *
     * @param timeout_ms number of milliseconds to wait before timing out
     * @return the result, once it is available
     * @throws TimeoutException if the result is not available after the default timeout set in the constructor
     */
    public T get(long timeout_ms) {
        T result = getOrNull(timeout_ms);
        if(isCanceled()) throw new CanceledException();
        if(result == null) throw new TimeoutException();
        else return result;
    }

    /**
     * Like {@link #get()}, but once the default timeout is reached - it will return null, instead of throwing an exception.
     * @return the result or null if it was not available in time
     */
    public T getOrNull() {
        return getOrNull(defaultTimeoutMs);
    }

    public void waitForIt() {
        get();
    }
    public void waitForIt(long timeToDaryInMs) {
        get(timeToDaryInMs);
    }

    /**
     * Like {@link #get(long)}, but once the timeout is reached - it will return null, instead of throwing an exception.
     * @return the result or null if it was not available in time
     */
    public T getOrNull(long timeout_ms) {
        if(isCanceled) return null;
        if(timeout_ms<0) timeout_ms=ENDLESS_WAIT;
        try {
            isGetWaiting=true;
            long waitingSince = System.currentTimeMillis();
            synchronized(this) {
                while(!isCompleted() && !isCanceled()) {
                    long elapsed = System.currentTimeMillis() - waitingSince;
                    if(timeout_ms - elapsed < 0) { //waiting for timeout without rechecking whether it has actually timed out is not possible - wait(timeout) is not guaranteed to sleep until timeout
                        timeout();
                        return null;
                    }
                    wait(timeout_ms);
                }
            }
            if(isCanceled) return null;
            return result.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            cancel();
            return null;
        } finally {
            isGetWaiting=false;
        }
    }

    /**
     * Sets the result. Notifies all still blocking waiting threads to wake up.
     *
     * Calls all unblocking waiters. Note that the callbacks are executed from this thread, i.e. if they hang this method will hang as well.
     *
     * @param result cannot be null.
     * @throws AlreadyCompletedException if the result was already set.
     */
    public void setCompleted(T result) {
//        System.out.println("P2LFuture.setCompleted: "+result);
        if(result == null) throw new NullPointerException("result cannot be null");
        if(isCanceled) throw new CanceledException();
        if(this.result.compareAndSet(null, result)) { //only if result previously not set
            if(isBlockingWaiting()) {
                synchronized(this) {
                    notifyAll();
                }
            }
            Consumer<T> unblockingWaiter;
            while((unblockingWaiter = resultCallbacks.pop()) != null) //at this point no more waiters are added, since the result is already set - however it would work anyways
                unblockingWaiter.accept(result);
        } else {
            throw new AlreadyCompletedException();
        }
    }

    public void cancel() {
        if(isCompleted()) throw new AlreadyCompletedException();
        isCanceled = true;
        if(!isCanceled && isBlockingWaiting()) {
            synchronized(this) {
                notifyAll();
            }
        }
        if(isUnblockingWaiting()) {
            resultCallbacks.clear();
        }
    }
    private void timeout() {
        hasTimedOut = true;
    }

    public P2LFuture<Boolean> toBooleanFuture(Function<T, Boolean> toBooleanConverter) {
        return toType(toBooleanConverter);
    }
    public <U> P2LFuture<U> toType(Function<T, U> toTypeConverter) {
        P2LFuture<U> f = new P2LFuture<>();
        callMeBack(t -> f.setCompleted(toTypeConverter.apply(t)));
//        System.out.println("toType - isUnblockingWaiting() = " + isUnblockingWaiting());
        return f;
//        return new P2LFuture<U>() {
//            @Override public U getOrNull(long timeout_ms) {
//                System.out.println("P2LFuture.toType.getOrNull");
//                isGetWaiting = true;
//                T t = P2LFuture.this.getOrNull(timeout_ms);
//                isGetWaiting = false;
//                System.out.println("P2LFuture.toType.getOrNull - t: "+t);
//                if(t==null) return null;
//                return toTypeConverter.apply(t);
//            }
//            @Override public void callMeBack(Consumer<U> callback) {
//                P2LFuture.this.callMeBack(t -> {
//                    if(isWaiting()) //only if anyone else is still waiting do the thing... If this future has timed out, do not do it
//                        callback.accept(toTypeConverter.apply(t));
//                });
//            }
//        isBlockingWaiting()
//        isUnblockingWaiting()
//        };
    }



    //CANNOT BE FED INTO OTHER COMBINE DIRECTLY - NEXT IS ONLY CALCULATED AFTER T IS AVAILABLE
    public <U> P2LFuture<U> combine(Function<T, P2LFuture<U>> next) {
        P2LFuture<U> f = new P2LFuture<>();
        callMeBack(t -> next.apply(t).callMeBack(f::setCompleted));
        return f;
//        return new P2LFuture<U>() { //does not properly work, because incorrect return of isWaiting etc...
//            @Override public U getOrNull(long timeout_ms) {
//                long before = System.currentTimeMillis();
//                T t = P2LFuture.this.getOrNull(timeout_ms);
//                if(t == null) return null;
//                long remaining_ms = timeout_ms - (System.currentTimeMillis()-before);
//                return next.apply(t).getOrNull(remaining_ms);
//            }
//
//            @Override public void callMeBack(Consumer<U> callback) {
//                P2LFuture.this.callMeBack(t -> {
//                    if(isWaiting()) //only if anyone else is still waiting do the thing... If this future has timed out, do not do it
//                        next.apply(t).callMeBack(this::setCompleted);
//                });
//            }
//        };
    }

    public static final BiFunction<Boolean, Boolean, Boolean> COMBINE_AND = (a, b) -> a && b;
    public static final BiFunction<Boolean, Boolean, Boolean> COMBINE_OR = (a, b) -> a || b;
    public static final BiFunction<Integer, Integer, Integer> COMBINE_PLUS = (a, b) -> a + b;
    public static final BiFunction<Integer, Integer, Integer> COMBINE_MINUS = (a, b) -> a - b;
    public static final BiFunction<Integer, Integer, Integer> COMBINE_MUL = (a, b) -> a * b;
    public <U, R> P2LFuture<R> combine(P2LFuture<U> next, BiFunction<T, U, R> combineFunction) {
        P2LFuture<R> f = new P2LFuture<>();
        callMeBack(t -> next.callMeBack(u -> f.setCompleted(combineFunction.apply(t, u))));
        return f;

//        return new P2LFuture<R>() { //does not properly work, because incorrect return of isWaiting etc...
//            @Override public R getOrNull(long timeout_ms) {
//                long before = System.currentTimeMillis();
//                T t = P2LFuture.this.getOrNull(timeout_ms);
//                if(t == null) return null;
//                long remaining_ms = timeout_ms - (System.currentTimeMillis()-before);
//                U u = next.getOrNull(remaining_ms);
//                if(u == null) return null;
//                return combineFunction.apply(t, u);
//            }
//
//            @Override public void callMeBack(Consumer<R> callback) {
//                P2LFuture.this.callMeBack(t -> {
//                    if(isWaiting()) //only if anyone else is still waiting do the thing... If this future has timed out, do not do it
//                        next.callMeBack(u -> setCompleted(combineFunction.apply(t, u)));
//                });
//            }
//        };
    }
    public static <T> P2LFuture<T> combine(Collection<P2LFuture<T>> futures, BiFunction<T, T, T> combineFunction) {
        P2LFuture<T> last = null;
        for(P2LFuture<T> next:futures) {
            if(last == null)
                last=next;
            else
                last = last.combine(next, combineFunction);
        }
        return last;
    }
}

