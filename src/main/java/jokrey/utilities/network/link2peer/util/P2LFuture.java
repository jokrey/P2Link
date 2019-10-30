package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.simple.data_structure.stack.LFStack;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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
 * @author jokrey
 */
public class P2LFuture<T> {
    public static final int ENDLESS_WAIT = 0;
    public static final int DEFAULT_TIMEOUT = ENDLESS_WAIT;

    private final long defaultTimeoutMs; //0 is endless wait...

    /**
     * Creates a new, not yet future.
     * <p>
     * The default timeout of its blocking methods is set to 'endless wait'.
     */
    public P2LFuture() {
        this(DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new, not yet completed future with the given default timeout in milliseconds.
     * This timeout is used by both {@link #get()} and {@link #getOrNull()}.
     * <p>
     * Note that this is not a guarantee the timeout is used, since the timeout can be overridden by the blocking wait methods.
     *
     * @param defaultTimeoutMs the default timeout in milliseconds - has to be greater or equal to 0, otherwise an IllegalArgumentException is thrown
     */
    public P2LFuture(long defaultTimeoutMs) {
        this.defaultTimeoutMs = defaultTimeoutMs;
        if (defaultTimeoutMs < 0)
            throw new IllegalArgumentException("timeout has to be >= 0");
    }

    /**
     * Creates a new, directly completed future.
     * When {@link #get()} or {@link #callMeBack(Consumer)} are called on this method, the will instantly present the result.
     * <p>
     * Useful when the future type is required, but the result is already available.
     *
     * @param result the instantly available result.
     */
    public P2LFuture(T result) {
        this(DEFAULT_TIMEOUT);
        this.result.set(result);
    }

    private volatile boolean isGetWaiting = false;
    private volatile boolean isCanceled = false;
    private volatile boolean hasTimedOut = false;
    private final LFStack<Consumer<T>> resultCallbacks = new LFStack<>();
    private final AtomicReference<T> result = new AtomicReference<>(null);

    /**
     * @return whether anyone is waiting for the future in a blocking fashion
     */
    private boolean isBlockingWaiting() {
        return isGetWaiting;
    } //outsider should not make a difference between blocking and unblocking waiting (type conversion and combine kinda rely on that)

    /**
     * @return whether anyone is waiting for the future in an unblocking fashion
     */
    private boolean isUnblockingWaiting() {
        return resultCallbacks.size() > 0;
    } //outsider should not make a difference between blocking and unblocking waiting (type conversion and combine kinda rely on that)

    /**
     * @return whether anyone is waiting for the future
     */
    public boolean isWaiting() {
        return !(isCanceled || isCompleted()) && (isBlockingWaiting() || isUnblockingWaiting());
    }

    /**
     * @return whether the result is available
     */
    public boolean isCompleted() {
        return getResult() != null;
    }

    /**
     * @return whether the result will ever be available
     */
    public boolean isCanceled() {
        return !isCompleted() && isCanceled;
    }

    /**
     * @return whether any blocking method of this future has ever timed out
     */
    public boolean hasTimedOut() {
        return hasTimedOut;
    }

    /**
     * @return the result or null if it is not yet available
     */
    public T getResult() {
        return result.get();
    }

    /**
     * If the result is already available, the callback will be called on the current thread immediately - before returning to the caller
     * If the result is not yet available, the callback will be called at any point in the future from the thread that sets the result as completed.
     * If the future is canceled the callback will be called with a null reference, a null reference will never be passed to the callback if the future is not canceled.
     * <p>
     * The order in which callbacks will be called is not strictly defined and may be subject to change. It should not be relied upon.
     *
     * @param callback callback receiving the result
     */
    public void callMeBack(Consumer<T> callback) {
        if (isCompleted())
            callback.accept(getResult());
        else
            resultCallbacks.push(callback);
    }

    /**
     * Waits for the result.
     * <p>
     * Times out with a TimeoutException, after the default number of ms set in the constructor
     * if no value was given in the constructor, get waits indefinitely until the result is available
     *
     * @return the result, once it is available
     * @throws TimeoutException if the result is not available after the default timeout set in the constructor
     */
    public T get() {
        return get(defaultTimeoutMs);
    }

    /**
     * Waits for the result.
     * <p>
     * Times out with a TimeoutException, after the number of milliseconds given
     * the given value has to be greater or equal to 0.
     * if the value is 0, this method will wait indefinitely until a result is available.
     *
     * @param timeout_ms number of milliseconds to wait before timing out
     * @return the result, once it is available
     * @throws TimeoutException if the result is not available after the default timeout set in the constructor
     */
    public T get(long timeout_ms) {
        T result = getOrNull(timeout_ms);
        if (isCanceled()) throw new CanceledException();
        if (result == null) {
            System.out.println("timeout_ms = " + timeout_ms);
            System.out.println("result = " + result);
            throw new TimeoutException();
        }
        return result;
    }

    /**
     * Like {@link #get()}, but once the default timeout is reached - it will return null, instead of throwing an exception.
     *
     * @return the result or null if it was not available in time
     */
    public T getOrNull() {
        return getOrNull(defaultTimeoutMs);
    }

    /**
     * Other name for get, which ignores the results.
     * In other words block until the result is available.
     */
    public void waitForIt() {
        get();
    }

    /**
     * Other name for get, which ignores the results.
     * In other words block until the result is available or it times out.
     * @param timeToDaryInMs time until timeout in milliseconds
     */
    public void waitForIt(long timeToDaryInMs) {
        get(timeToDaryInMs);
    }

    /**
     * Like {@link #get(long)}, but once the timeout is reached - it will return null, instead of throwing an exception.
     * @return the result or null if it was not available in time
     */
    public T getOrNull(long timeout_ms) {
        if (isCanceled()) return null;
        if (timeout_ms < 0) timeout_ms = ENDLESS_WAIT;
        try {
            isGetWaiting = true;
            long waitingSince = System.currentTimeMillis();
            synchronized (this) {
                while (!isCompleted() && !isCanceled()) {
                    long elapsed_ms = System.currentTimeMillis() - waitingSince;
                    long remaining_ms = timeout_ms <= 0 ? ENDLESS_WAIT : timeout_ms - elapsed_ms;
                    if (remaining_ms < 0) { //waiting for timeout without rechecking whether it has actually timed out is not possible - wait(timeout) is not guaranteed to sleep until timeout
                        timeout();
                        return null;
                    }
                    wait(remaining_ms);
                }
            }
            if (isCanceled())
                return null;
            return getResult();
        } catch (InterruptedException e) {
            e.printStackTrace();
            cancel();
            return null;
        } finally {
            isGetWaiting = false;
        }
    }

    /**
     * Sets the result. Notifies all still blocking waiting threads to wake up.
     * Calls all unblocking waiters. Note that the callbacks are executed from this thread, i.e. if they hang this method will hang as well.
     *
     * @param result cannot be null.
     * @throws AlreadyCompletedException if the result was already set.
     */
    public void setCompleted(T result) {
        if (result == null) throw new NullPointerException("result cannot be null");
        if (isCanceled()) throw new CanceledException();
        if (this.result.compareAndSet(null, result)) { //only if result previously not set
            if (isBlockingWaiting()) {
                synchronized (this) {
                    notifyAll();
                }
            }
            Consumer<T> unblockingWaiter;
            while ((unblockingWaiter = resultCallbacks.pop()) != null) //at this point no more waiters are added, since the result is already set - however it would work anyways
                unblockingWaiter.accept(result);
        } else {
            throw new AlreadyCompletedException();
        }
    }

    /**
     * Cancels the future, making is impossible to set it to complete ever again.
     * Notifies all still blocking waiting threads to wake up. They will throw {@link CanceledException}.
     * Calls all unblocking waiters with a null reference. Note that the callbacks are executed from this thread, i.e. if they hang this method will hang as well.
     *
     * @throws AlreadyCompletedException if the future was completed before this method ends
     */
    public void cancel() {
        if (isCompleted()) throw new AlreadyCompletedException();
        isCanceled = true;
        if (isBlockingWaiting()) {
            synchronized (this) {
                notifyAll();
            }
        }
        Consumer<T> unblockingWaiter;
        while ((unblockingWaiter = resultCallbacks.pop()) != null) //at this point no more waiters are added, since the result is already set - however it would work anyways
            unblockingWaiter.accept(getResult()); //if the result was set in the meantime - this will respect that
        if (isCompleted()) throw new AlreadyCompletedException();
    }

    private void timeout() {
        hasTimedOut = true;
    }

    /**
     * Converts the given future to a boolean future using the given converter function
     *
     * @param converter T to Boolean converter function
     * @return the newly created future of type boolean
     */
    public P2LFuture<Boolean> toBooleanFuture(Function<T, Boolean> converter) {
        return toType(converter);
    }

    /**
     * Converts the given future to a future of type R using the given converter function
     * @param converter T to R converter function
     * @return the newly created future of type R
     */
    public <R> P2LFuture<R> toType(Function<T, R> converter) {
        P2LFuture<R> f = new P2LFuture<>();
        callMeBack(t -> f.setCompletedOrCanceledBasedOn(t == null ? null : converter.apply(t)));
        return f;
    }


    /**
     * Creates a new future that represents a combination of this future and the future created by the given function.
     * The function is only called when and if this future completes. If either future are canceled the creating future is canceled.
     *
     * This method is especially useful to create retrieable conversations in a p2l application.
     * Since the given function takes the input of the this future,
     *   this can be used to receive intermediate results from the other peer and use it to calculate the next conversation step.
     * @param next function that calculates the next future, with which this future will be combined
     * @return the newly created, combined future.
     */
    public <U> P2LFuture<U> combine(Function<T, P2LFuture<U>> next) {
        //CANNOT BE FED INTO OTHER COMBINE DIRECTLY - NEXT IS ONLY CALCULATED AFTER T IS AVAILABLE
        P2LFuture<U> f = new P2LFuture<>();
        callMeBack(t -> {
            if (t == null) f.cancel();
            else next.apply(t).callMeBack(f::setCompletedOrCanceledBasedOn);
        });
        return f;
    }

    /** if the given result is null, this future is canceled - if it is not null, this future is set to completed. */
    protected void setCompletedOrCanceledBasedOn(T resultOrNull) {
        if (resultOrNull == null) cancel();
        else setCompleted(resultOrNull);
    }

    /** Build in reduce or combine function for Booleans (using and) */
    public static final BiFunction<Boolean, Boolean, Boolean> AND = (a, b) -> a && b;
    /** Build in reduce or combine function for Booleans (using or) */
    public static final BiFunction<Boolean, Boolean, Boolean> OR = (a, b) -> a || b;
    /** Build in reduce or combine function for Integers (using +) */
    public static final BiFunction<Integer, Integer, Integer> PLUS = (a, b) -> a + b;
    /** Build in reduce or combine function for Integers (using -) */
    public static final BiFunction<Integer, Integer, Integer> MINUS = (a, b) -> a - b;
    /** Build in reduce or combine function for Integers (using *) */
    public static final BiFunction<Integer, Integer, Integer> MUL = (a, b) -> a * b;

    /**
     * Creates a new future that combines this and the given 'next' future.
     * The newly created future is only set to complete if both this and the 'next' future are set to complete.
     * If either future is canceled, the created future is also canceled.
     * The two results are combined using a combine function.
     * The combine function can simply create a pair of the two results, only return one of the results or do something more complex.
     * @param next the next future to be combined with this future
     * @param combineFunction function combining types T and U to R
     * @return the newly created future that combines this and the 'next' future
     */
    public <U, R> P2LFuture<R> combine(P2LFuture<U> next, BiFunction<T, U, R> combineFunction) {
        P2LFuture<R> f = new P2LFuture<>();
        callMeBack(t -> {
            if (t == null) f.cancel();
            else next.callMeBack(u -> {
                if (u == null) f.cancel();
                else f.setCompleted(combineFunction.apply(t, u));
            });
        });
        return f;
    }

    /**
     * Reduces the given collection of futures to a single future using the given reduce function.
     * If any of the futures are canceled the entire reduce future is canceled.
     * @param futures collection of futures of the same type
     * @param reduceFunction reduce function
     * @return the newly created reduce future
     */
    public static <T> P2LFuture<T> reduce(Collection<P2LFuture<T>> futures, BiFunction<T, T, T> reduceFunction) {
        P2LFuture<T> last = null;
        for(P2LFuture<T> next:futures) {
            if(last == null)
                last=next;
            else
                last = last.combine(next, reduceFunction);
        }
        return last;
    }
    /**
     * Reduces the given collection of futures to a single future using the given reduce function.
     * If any of the futures are canceled they are not included in the resulting reduce.
     * Only if all the futures are canceled, the reduce future is canceled - otherwise the canceled result is simply never passed to the reduce function.
     * The order in which the reduce function is called is not predictable.
     * @param futures collection of futures of the same type
     * @param reduceFunction reduce function
     * @return the newly created reduce future
     */
    public static <T> P2LFuture<T> reduceWhenCompleted(Collection<P2LFuture<T>> futures, BiFunction<T, T, T> reduceFunction) {
        return oneForAll(futures).toType(ts -> {
            T last = null;
            for(T next:ts) {
                if(last == null)
                    last=next;
                else
                    last = reduceFunction.apply(last, next);
            }
            return last;
        });
    }

    /**
     * Converts the collection of futures into a future of a collections.
     * The resulting future is set to completed if all given futures are completed or canceled.
     * The results of canceled futures are naturally not included in the resulting collection.
     * The resulting future is never canceled. Even if all original futures are canceled the resulting future is just set completed with an empty collection.
     * The order in the given and resulting collection are not necessarily in the same order.
     * @param futures collection of futures of the same type
     * @return the newly created future for the collection of results
     */
    public static <T> P2LFuture<Collection<T>> oneForAll(Collection<P2LFuture<T>> futures) {
        int origSize = futures.size();
        AtomicInteger counter = new AtomicInteger(0);
        LinkedList<T> collector = new LinkedList<>();

        P2LFuture<Collection<T>> allResults = new P2LFuture<>();
        for(P2LFuture<T> future:futures)
            future.callMeBack(e -> {
                synchronized(collector) {
                    if(e != null) collector.push(e);
                }

                int numberOfCompletedFutures = counter.incrementAndGet();
                if(numberOfCompletedFutures == origSize)
                    allResults.setCompleted(collector);
            });
        if(futures.isEmpty())
            allResults.setCompleted(collector);
        return allResults;
    }

    /**
     * Like {@link #reduceWhenCompleted(Collection, BiFunction)}, but additionally applies a conversion function.
     * Only if all the futures are canceled, the reduce future is canceled - otherwise the canceled result is simply never passed to the reduce function.
     * The order in which the reduce function is called is not predictable.
     * @param futures collection of futures of the same type
     * @param converter conversion function
     * @param reduceFunction reduce function
     * @return the newly created reduce future
     */
    public static <T, R> P2LFuture<R> reduceConvertWhenCompleted(Collection<P2LFuture<T>> futures, Function<T, R> converter, BiFunction<R, R, R> reduceFunction) {
        return oneForAll(futures).toType(ts -> {
            R last = null;
            for(T next:ts) {
                if(last == null)
                    last = converter.apply(next);
                else
                    last = reduceFunction.apply(last, converter.apply(next));
            }
            return last;
        });
    }
}

