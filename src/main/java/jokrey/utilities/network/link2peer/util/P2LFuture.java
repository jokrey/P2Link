package jokrey.utilities.network.link2peer.util;

import jokrey.utilities.network.link2peer.util.P2LThreadPool.Task;
import jokrey.utilities.simple.data_structure.pairs.Pair;
import jokrey.utilities.simple.data_structure.stack.LinkedStack;
import jokrey.utilities.simple.data_structure.stack.Stack;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
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
        this.result = result;
    }

    private volatile boolean isGetWaiting = false;
    private volatile boolean isCanceled = false;
    private volatile boolean hasTimedOut = false;
    private final Stack<Consumer<T>> resultCallbacks = new LinkedStack<>();
    private T result = null;

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
        return resultCallbacks.size() > 0; //thread safe, even without blocking, due to copy on write (though result may not be perfectly current)
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
        return result;
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
    public synchronized void callMeBack(Consumer<T> callback) {
        //has to be synchronized, otherwise between pushing the callback and the isCompleted - it could be completed and never called....
        if (isCompleted())
            callback.accept(getResult());
        else {
            resultCallbacks.push(callback);
        }
    }

    /**
     * Waits for the result.
     * <p>
     * Times out with a TimeoutException, after the default number of ms set in the constructor
     * if no value was given in the constructor, get waits indefinitely until the result is available
     *
     * @return the result, once it is available
     * @throws CanceledException if the future was canceled
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
     * @throws CanceledException if the future was canceled
     * @throws TimeoutException if the result is not available after the given timeout
     */
    public T get(long timeout_ms) {
        T result = getOrNull(timeout_ms);
        if (isCanceled()) throw new CanceledException();
        if (result == null) throw new TimeoutException();
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
     * @throws CanceledException if the future was canceled
     * @throws TimeoutException if the result is not available after the default timeout set in the constructor
     */
    public void waitForIt() {
        get();
    }

    /**
     * Other name for get, which ignores the results.
     * In other words block until the result is available or it times out.
     * @param timeToDaryInMs time until timeout in milliseconds
     * @throws CanceledException if the future was canceled
     * @throws TimeoutException if the result is not available after the given timeout
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
        if (isCompleted()) return getResult();
        if (timeout_ms < 0) timeout_ms = ENDLESS_WAIT;
        try {
            isGetWaiting = true;

            if(SyncHelp.waitUntil(this, () -> isCompleted() || isCanceled(), timeout_ms)) {
                if(isCanceled())
                    return null;
                return getResult();
            } else {
                timeout();
                return null;
            }
        } finally {
            isGetWaiting = false;
        }
    }

    void overrideResult(T newResult) {
        if (newResult == null) throw new NullPointerException("result cannot be null");
        synchronized (this) {
            if (isCanceled()) throw new CanceledException();
            this.result = newResult;
            done();
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
        synchronized (this) {
            if (isCanceled()) throw new CanceledException();
            if (isCompleted()) throw new AlreadyCompletedException();
            this.result = result;
            done();
        }
    }

    /**
     * Cancels the future, making is impossible to set it to complete ever again.
     * Notifies all still blocking waiting threads to wake up. They will throw a {@link CanceledException}.
     * Calls all unblocking waiters with a null reference. Note that the callbacks are executed from this thread, i.e. if they hang this method will hang as well.
     *
     * If the future was previously canceled, this method will do nothing.
     *
     * @throws AlreadyCompletedException if the future was completed before this method ends
     */
    public void cancel() {
        synchronized (this) {
            if (isCompleted()) throw new AlreadyCompletedException();
            if(isCanceled()) return;
            isCanceled = true;
            done();
        }
    }

    /**
     * Like {@link #cancel()} except it won't even throw an exception
     * @return !isCompleted() && !isCanceled()
     */
    public boolean cancelIfNotCompleted() {
        synchronized (this) {
            if(isCompleted()) return false;
            if(isCanceled()) return false;
            isCanceled = true;
            done();
            return true;
        }
    }

    private synchronized void done() {
        if (isBlockingWaiting())
            notifyAll();

        Consumer<T> unblockingWaiter;
        while ((unblockingWaiter = resultCallbacks.pop()) != null) //at this point no more waiters are added, since the result is already set - however it would work anyways
            unblockingWaiter.accept(getResult()); //if the result was set in the meantime - this will respect that
    }
    private void timeout() {
        hasTimedOut = true;
    }


    /**
     * Despite its name this method is kinda meant to do the opposite than run the task before.
     * Since the parameters of a method in java are NOT calculated lazily the future is actually waiting before the task is run.
     * If the future depends on the task having been run (for example an expecting future for an instantly expiring message), then this is the desired behaviour.
     * Internally it works by running {@link #nowOrCancel(Task)} on the given future with the given task. The advantage is the more intuitive semantic order of the actions.
     * @param task a given task to execute immediately (before or after the future is complete, but after it exists and can be completed)
     * @param future a future
     * @return the given future itself to allow for chained calls
     * @throws Throwable if the given task throws an exception that exception is rethrown
     */
    public static <T> P2LFuture<T> before(Task task, P2LFuture<T> future) throws Throwable {
        return future.nowOrCancel(task);
    }

    /**
     * Executes the given task instantly.
     * Useful when this future has to exist for the task to be safely executed(for example because it cause the future to be conceptually complete),
     *     but it has to returned from a method.
     * @param task a given task to execute immediately (before or after the future is complete, but after it exists and can be completed)
     * @throws Throwable any given exception the task throws is rethrown and causes this future to be canceled
     * @return this future to allow for chained calls
     * @throws Throwable if the given task throws an exception that exception is rethrown
     */
    public P2LFuture<T> nowOrCancel(Task task) throws Throwable {
        try {
            task.run();
        } catch (Throwable t) {
            cancel();
            throw t;
        }
        return this;
    }
    /**
     * Like {@link #nowOrCancel(Task)}, except the task cannot throw an exception and will not cause this future to be canceled.
     * RuntimeExceptions are simply rethrown and do not cause this future to be canceled.
     * @see #nowOrCancel(Task)
     */
    public P2LFuture<T> now(Runnable task) {
        task.run();
        return this;
    }
    /**
     * The given task will be executed after the future completes OR is canceled.
     * Essentially a wrapper for {@link #callMeBack(Consumer)} that does not consider the result of this future and returns this.
     * @param task a given task to be executed after the future completes OR is canceled
     * @return this future
     */
    public P2LFuture<T> then(Runnable task) {
        callMeBack(t -> task.run());
        return this;
    }
    /**
     * The given task will be executed after the future completes, but ONLY if it completes successfully (i.e. if the future is canceled the task will not be called)
     * @param task a given task to be executed after the future completes
     * @return this future
     */
    public P2LFuture<T> whenCompleted(Runnable task) {
        callMeBack(t -> { if(t != null) task.run(); });
        return this;
    }
    /**
     * The given task will be executed after the future is canceled (i.e. if the future is completed successfully the task will not be called)
     * @param task a given task to be executed after the future is canceled
     * @return this future
     */
    public P2LFuture<T> whenCanceled(Runnable task) {
        callMeBack(t -> { if(t == null) task.run(); });
        return this;
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
     * Converts the this future to a future of type R using the given converter function
     * If the returned future is canceled, the underlying (i.e. this) future is also canceled.
     * If the returned future is completed directly(not automatically through completing the underlying future), then behaviour is undefined.
     *
     * The returned future will ALWAYS be waited upon, additionally the original future will automatically be waited upon from now on
     *    unlike before, where {@link #isWaiting()} could be used to determine whether the 'user' is still waiting on it - this is no longer possible
     *
     * @param converter T to R converter function
     * @return the newly created future of type R
     */
    public <R> P2LFuture<R> toType(Function<T, R> converter) {
        P2LFuture<R> f = new P2LFuture<>();
        callMeBack(t -> f.setCompletedOrCanceledBasedOn(t == null ? null : converter.apply(t)));
        f.callMeBack(t -> { if(t == null) cancelIfNotCompleted(); }); //if f is canceled, the underlying needs to be canceled as well - might be a short loop(i.e. the other callback canceled f, but that is not a problem)
        return f;
    }


    /**
     * Creates a new future that represents a combination of this future and the future created by the given function.
     * The function is only called when and if this future completes. If either future are canceled the creating future is canceled.
     *
     * This method is especially useful to create retryable conversations in a p2l application.
     * Since the given function takes the input of the this future,
     *   this can be used to receive intermediate results from the other peer and use it to calculate the next conversation step.
     * @param next function that calculates the next future, with which this future will be combined
     * @return the newly created, combined future.
     */
    public <U> P2LFuture<U> andThen(Function<T, P2LFuture<U>> next) {
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

    /** Build in reduce or andThen function for Booleans (using and) */
    public static final BiFunction<Boolean, Boolean, Boolean> AND = (a, b) -> a && b;
    /** Build in reduce or andThen function for Booleans (using or) */
    public static final BiFunction<Boolean, Boolean, Boolean> OR = (a, b) -> a || b;
    /** Build in reduce or andThen function for Integers (using +) */
    public static final BiFunction<Integer, Integer, Integer> PLUS = (a, b) -> a + b;
    /** Build in reduce or andThen function for Integers (using -) */
    public static final BiFunction<Integer, Integer, Integer> MINUS = (a, b) -> a - b;
    /** Build in reduce or andThen function for Integers (using *) */
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
    public static <T, F extends P2LFuture<T>> P2LFuture<Collection<T>> oneForAll(Collection<F> futures) {
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

    public static <T, U> P2LFuture<Pair<T, U>> combineOr(P2LFuture<T> previous, P2LFuture<U> next) {
        P2LFuture<Pair<T, U>> f = new P2LFuture<>();
        previous.callMeBack(t -> {
            Pair<T, U> previousResult = f.getResult();
            if(previousResult == null)
                f.overrideResult(new Pair<>(t, null));
            else
                f.overrideResult(new Pair<>(t, previousResult.r));

        });
        next.callMeBack(u -> {
            Pair<T, U> previousResult = f.getResult();
            if(previousResult == null)
                f.overrideResult(new Pair<>(null, u));
            else
                f.overrideResult(new Pair<>(previousResult.l, u));
        });
        f.callMeBack(t -> { if(t == null) {
            previous.cancelIfNotCompleted();
            next.cancelIfNotCompleted();
        } }); //if f is canceled, the underlying needs to be canceled as well - might be a short loop(i.e. the other callback canceled f, but that is not a problem)
        return f;
    }

    /**
     * Waits for the given futures or throws an exception if not all are available in time
     * @param timeoutMs maximum time to wait, if not all results are available in time an exception will be thrown
     * @param futures futures to wait for
     * @throws TimeoutException if not all results are available in time
     */
    public static void waitForEm(int timeoutMs, P2LFuture<?>... futures) {
        long startTime = System.currentTimeMillis();
        for(P2LFuture<?> future:futures) {
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = timeoutMs - elapsed;
            if (remaining > 0)
                future.waitForIt(remaining);
            else
                throw new TimeoutException();
        }
    }

    /**
     * Waits for the given collection of futures for the given total time until a timeout.
     * Within this timeout the method will collect as many results as it can.
     * If a future in the given collection is canceled, this method will ignore it.
     * All futures in the given collection that do not complete by the timeout are marked to have timed out.
     * @param futures collection of futures of the same type
     * @param timeoutMs maximum time until this method returns
     * @return a list of obtained results, min size is 0, max size is == futures.size
     */
    public static <T> Collection<T> waitForEm(Collection<P2LFuture<T>> futures, int timeoutMs) {
        LinkedList<T> list = new LinkedList<>();
        long startTime = System.currentTimeMillis();
        for(P2LFuture<T> future:futures) {
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = timeoutMs-elapsed;
            if(remaining > 0) {
                T result = future.getOrNull(remaining);
                if(result != null)
                    list.add(result);
                //do not break if null, could just be this single future was canceled
            } else {
                T result = future.getResult();//do not wait anymore
                if(result != null)
                    list.add(result);
                else
                    future.hasTimedOut();
            }
        }
        return list;
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

