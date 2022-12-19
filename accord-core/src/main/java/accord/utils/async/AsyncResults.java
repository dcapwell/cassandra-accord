package accord.utils.async;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class AsyncResults
{
    private AsyncResults() {}

    private static class Result<V>
    {
        final V value;
        final Throwable failure;

        public Result(V value, Throwable failure)
        {
            this.value = value;
            this.failure = failure;
        }
    }

    static class AbstractResult<V> implements AsyncResult<V>
    {
        private static final AtomicReferenceFieldUpdater<AbstractResult, Object> STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractResult.class, Object.class, "state");

        private volatile Object state;

        private static class Listener<V>
        {
            final BiConsumer<? super V, Throwable> callback;
            Listener<V> next;

            public Listener(BiConsumer<? super V, Throwable> callback)
            {
                this.callback = callback;
            }
        }

        private void notify(Listener<V> listener, Result<V> result)
        {
            while (listener != null)
            {
                listener.callback.accept(result.value, result.failure);
                listener = listener.next;
            }
        }

        boolean trySetResult(Result<V> result)
        {
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                    return false;
                Listener<V> listener = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, result))
                {
                    notify(listener, result);
                    return true;
                }
            }
        }

        boolean trySetResult(V result, Throwable failure)
        {
            return trySetResult(new Result<>(result, failure));
        }

        void setResult(Result<V> result)
        {
            if (!trySetResult(result))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        void setResult(V result, Throwable failure)
        {
            if (!trySetResult(result, failure))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        @Override
        public void addCallback(BiConsumer<? super V, Throwable> callback)
        {
            Listener<V> listener = null;
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                {
                    Result<V> result = (Result<V>) current;
                    callback.accept(result.value, result.failure);
                    return;
                }
                if (listener == null)
                    listener = new Listener<>(callback);

                listener.next = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, listener))
                    return;
            }
        }

        @Override
        public boolean isDone()
        {
            return state instanceof Result;
        }

        @Override
        public boolean isSuccess()
        {
            Object current = state;
            return current instanceof Result && ((Result) current).failure == null;
        }
    }

    static class Chain<V> extends AbstractResult<V>
    {
        public Chain(AsyncChain<V> chain)
        {
            chain.begin(this::setResult);
        }
    }

    public static class Settable<V> extends AbstractResult<V> implements AsyncResult.Settable<V>
    {

        @Override
        public boolean trySuccess(V value)
        {
            return trySetResult(value, null);
        }

        @Override
        public boolean tryFailure(Throwable throwable)
        {
            return trySetResult(null, throwable);
        }
    }

    static class Immediate<V> implements AsyncResult<V>
    {
        private final V value;
        private final Throwable failure;

        Immediate(V value)
        {
            this.value = value;
            this.failure = null;
        }

        Immediate(Throwable failure)
        {
            this.value = null;
            this.failure = failure;
        }

        @Override
        public void addCallback(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(value, failure);
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public boolean isSuccess()
        {
            return failure == null;
        }
    }

    /**
     * Creates an AsyncResult for the given chain. This calls begin on the supplied chain
     */
    public static <V> AsyncResult<V> forChain(AsyncChain<V> chain)
    {
        return new Chain<>(chain);
    }

    public static <V> AsyncResult<V> success(V value)
    {
        return new Immediate<>(value);
    }

    public static <V> AsyncResult<V> failure(Throwable failure)
    {
        return new Immediate<>(failure);
    }

    public static <V> AsyncResult<V> ofCallable(Executor executor, Callable<V> callable)
    {
        Settable<V> result = new Settable<V>();
        executor.execute(() -> {
            try
            {
                result.trySuccess(callable.call());
            }
            catch (Exception e)
            {
                result.tryFailure(e);
            }
        });
        return result;
    }

    public static AsyncResult<Void> ofRunnable(Executor executor, Runnable runnable)
    {
        Settable<Void> result = new Settable<Void>();
        executor.execute(() -> {
            try
            {
                runnable.run();
                result.trySuccess(null);
            }
            catch (Exception e)
            {
                result.tryFailure(e);
            }
        });
        return result;
    }

    public static <V> AsyncResult.Settable<V> settable()
    {
        return new Settable<>();
    }

    private static <V> List<AsyncChain<V>> toChains(List<AsyncResult<V>> results)
    {
        List<AsyncChain<V>> chains = new ArrayList<>(results.size());
        for (int i=0,mi=results.size(); i<mi; i++)
            chains.add(results.get(i).toChain());
        return chains;
    }

    public static <V> AsyncChain<List<V>> all(List<AsyncResult<V>> results)
    {
        Preconditions.checkArgument(!results.isEmpty());
        return new AsyncChainCombiner.All<>(toChains(results));
    }

    public static <V> AsyncChain<V> reduce(List<AsyncResult<V>> results, BiFunction<V, V, V> reducer)
    {
        Preconditions.checkArgument(!results.isEmpty());
        if (results.size() == 1)
            return results.get(0).toChain();
        return new AsyncChainCombiner.Reduce<>(toChains(results), reducer);
    }

    public static <V> V getBlocking(AsyncResult<V> asyncResult) throws InterruptedException, ExecutionException
    {
        AtomicReference<Result<V>> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        asyncResult.addCallback((result, failure) -> {
            callbackResult.set(new Result<>(result, failure));
            latch.countDown();
        });

        latch.await();
        Result<V> result = callbackResult.get();
        if (result.failure == null) return result.value;
        else throw new ExecutionException(result.failure);
    }

    public static <V> V getBlocking(AsyncResult<V> asyncResult, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        AtomicReference<Result<V>> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        asyncResult.addCallback((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        if (!latch.await(timeout, unit))
            throw new TimeoutException();
        Result<V> result = callbackResult.get();
        if (result.failure == null) return result.value;
        else throw new ExecutionException(result.failure);
    }

    public static <V> V getUninterruptibly(AsyncResult<V> asyncResult)
    {
        try
        {
            return getBlocking(asyncResult);
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <V> void awaitUninterruptibly(AsyncResult<V> asyncResult)
    {
        getUninterruptibly(asyncResult);
    }
}
