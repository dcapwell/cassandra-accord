package accord.utils.async;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Handle for async computations that supports multiple listeners and registering
 * listeners after the computation has started
 */
public interface AsyncResult<V>
{
    void listen(BiConsumer<? super V, Throwable> callback, @Nullable Executor executor);

    default void listen(BiConsumer<? super V, Throwable> callback)
    {
        listen(callback, null);
    }

    default void listen(Runnable runnable)
    {
        listen((unused, failure) -> {
            if (failure == null) runnable.run();
            else throw new RuntimeException(failure);
        });
    }

    default void listen(Runnable runnable, Executor executor)
    {
        listen(AsyncCallbacks.inExecutor(runnable, executor));
    }

    default <T> AsyncResult<T> map(Function<? super V, ? extends T> mapper, Executor executor)
    {
        AsyncResult.Settable<T> settable = AsyncResults.settable();
        listen((success, failure) -> {
            if (failure != null)
            {
                settable.setFailure(failure);
                return;
            }
            try
            {
                T result = mapper.apply(success);
                settable.setSuccess(result);
            }
            catch (Throwable t)
            {
                settable.setFailure(t);
            }
        }, executor);
        return settable;
    }

    default <T> AsyncResult<T> map(Function<? super V, ? extends T> mapper)
    {
        return map(mapper, null);
    }

    default <T> AsyncResult<T> flatMap(Function<? super V, ? extends AsyncResult<T>> mapper, Executor executor)
    {
        AsyncResult.Settable<T> settable = AsyncResults.settable();
        listen((success, failure) -> {
           if (failure != null)
           {
               settable.setFailure(failure);
               return;
           }
           try
           {
               AsyncResult<T> next = mapper.apply(success);
               next.listen((s2, f2) -> {
                   if (f2 != null)
                   {
                       settable.tryFailure(f2);
                       return;
                   }
                   settable.trySuccess(s2);
               });
           }
           catch (Throwable t)
           {
               settable.setFailure(t);
           }
        }, executor);
        return settable;
    }

    default <T> AsyncResult<T> flatMap(Function<? super V, ? extends AsyncResult<T>> mapper)
    {
        return flatMap(mapper, null);
    }

    default AsyncChain<V> toChain()
    {
        return new AsyncChains.Head<V>()
        {
            @Override
            public void begin(BiConsumer<? super V, Throwable> callback)
            {
                listen(callback);
            }
        };
    }

    boolean isDone();
    boolean isSuccess();

    default void addCallback(BiConsumer<? super V, Throwable> callback)
    {
        listen(callback);
    }

    default void addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        listen(callback, executor);
    }

    default void addListener(Runnable runnable)
    {
        listen(runnable);
    }

    default void addListener(Runnable runnable, Executor executor)
    {
        listen(runnable, executor);
    }

    interface Settable<V> extends AsyncResult<V>
    {
        boolean trySuccess(V value);

        default void setSuccess(V value)
        {
            if (!trySuccess(value))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        boolean tryFailure(Throwable throwable);

        default void setFailure(Throwable throwable)
        {
            if (!tryFailure(throwable))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        default BiConsumer<V, Throwable> settingCallback()
        {
            return (result, throwable) -> {

                if (throwable == null)
                    trySuccess(result);
                else
                    tryFailure(throwable);
            };
        }
    }
}
