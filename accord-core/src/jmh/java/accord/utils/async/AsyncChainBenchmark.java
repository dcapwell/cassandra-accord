package accord.utils.async;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
// To run faster, uncomment the below
//@Fork(1)
//@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
//@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
public class AsyncChainBenchmark {
    @Param({"1", "2", "3", "4", "5"})
    int chainSize;

    @Benchmark
    public AsyncResult<Integer> chainMap()
    {
        AsyncChain<Integer> accum = AsyncResults.success(0).toChain();
        for (int i = 0; i < chainSize; i++)
            accum = accum.map(AsyncChainBenchmark::map);
        return accum.beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMap()
    {
        AsyncChain<Integer> accum = AsyncResults.success(0).toChain();
        for (int i = 0; i < chainSize; i++)
            accum = accum.flatMap(a -> flatMap(a).toChain());
        return accum.beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> resultMap()
    {
        AsyncResult<Integer> accum = AsyncResults.success(0);
        for (int i = 0; i < chainSize; i++)
            accum = accum.map(AsyncChainBenchmark::map);
        return accum;
    }

    @Benchmark
    public AsyncResult<Integer> resultFlatMap()
    {
        AsyncResult<Integer> accum = AsyncResults.success(0);
        for (int i = 0; i < chainSize; i++)
            accum = accum.flatMap(AsyncChainBenchmark::flatMap);
        return accum;
    }


    @Benchmark
    public AsyncResult<Integer> chainMapAndBack()
    {
        AsyncResult<Integer> accum = AsyncResults.success(0);
        for (int i = 0; i < chainSize; i++)
            accum = accum.toChain().map(AsyncChainBenchmark::map).beginAsResult();
        return accum;
    }

    @Benchmark
    public AsyncResult<Integer> chainFlatMapAndBack()
    {
        AsyncResult<Integer> accum = AsyncResults.success(0);
        for (int i = 0; i < chainSize; i++)
            accum = accum.toChain().flatMap(a -> flatMap(a).toChain()).beginAsResult();
        return accum;
    }

    private static <A> A map(A a) {
        return a;
    }

    private static <A> AsyncResult<A> flatMap(A a) {
        // avoid Immediate as the overhead is less there
        AsyncResults.Settable<A> promise = new AsyncResults.Settable<>();
        promise.setSuccess(a);
        return promise;
    }
}
