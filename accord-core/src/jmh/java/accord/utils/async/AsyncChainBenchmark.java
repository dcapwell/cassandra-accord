package accord.utils.async;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class AsyncChainBenchmark {
    @Param({"10"})
    int chainSize;

    @Benchmark
    public AsyncResult<Integer> chain()
    {
        AsyncChain<Integer> accum = AsyncResults.success(0).toChain();
        for (int i = 0; i < chainSize; i++)
            accum = accum.map(AsyncChainBenchmark::map);
        return accum.beginAsResult();
    }

    @Benchmark
    public AsyncResult<Integer> result()
    {
        AsyncResult<Integer> accum = AsyncResults.success(0);
        for (int i = 0; i < chainSize; i++)
            accum = accum.map(AsyncChainBenchmark::map);
        return accum;
    }

    @Benchmark
    public AsyncResult<Integer> chainAndBack()
    {
        AsyncResult<Integer> accum = AsyncResults.success(0);
        for (int i = 0; i < chainSize; i++)
            accum = accum.toChain().map(AsyncChainBenchmark::map).beginAsResult();
        return accum;
    }

    private static <A> A map(A a) {
        return a;
    }
}
