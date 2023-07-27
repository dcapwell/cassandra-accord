/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.topology;

import accord.local.Node;
import accord.utils.IndexedFunction;
import accord.utils.Invariants;

import java.util.AbstractList;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class ShardsArray<T> extends AbstractList<T>
{
    public interface Factory<T>
    {
        T apply(int epochIndex, Topology topology, Shard shard);
    }

    private final Topologies topologies;
    private final T[] array;
    private final int[] offsets;

    public ShardsArray(Topologies topologies, IntFunction<T[]> arrayFactory)
    {
        this(topologies, arrayFactory, (i1, i2, i3) -> null);
    }

    public ShardsArray(Topologies topologies, IntFunction<T[]> arrayFactory, Factory<T> factory)
    {
        this.topologies = topologies;
        int[] sizes = IntStream.range(0, topologies.size()).map(i -> topologies.get(i).size()).toArray();
        this.offsets = new int[sizes.length];
        offsets[0] = 0;
        for (int i = 1; i < sizes.length; i++)
            offsets[i] = offsets[i - 1] + sizes[i - 1];
        this.array = arrayFactory.apply(IntStream.of(sizes).sum());
        for (int i = 0; i < topologies.size(); i++)
        {
            Topology t = topologies.get(i);
            int offset = offsets[i];
            int size = t.size();
            for (int j = 0; j < size; j++)
                array[offset + j] = factory.apply(i, t, t.get(j));
        }
    }

    @Override
    public T get(int index)
    {
        return array[index];
    }

    public T get(int epochIndex, int shardIndex)
    {
        int offset = offsets[epochIndex];
        Topology t = topologies.get(epochIndex);
        Invariants.checkArgument(shardIndex >= 0 && shardIndex < t.size(), "shard index %d is not between 0 and %d", shardIndex, t.size());
        return get(offset + shardIndex);
    }

    @Override
    public T set(int index, T element)
    {
        T current = array[index];
        array[index] = element;
        return current;
    }

    public int offset(int index)
    {
        return offsets[index];
    }

    @Override
    public int size()
    {
        return array.length;
    }

    public <A> A mapReduce(Node.Id from,
                           IndexedFunction<? super T, ? extends A> map,
                           BiFunction<? super A, ? super A, ? extends A> reduce,
                           A zero)
    {
        return mapReduce(0, topologies.size(), from, map, reduce, zero, ignore -> false);
    }

    public <A> A mapReduce(Node.Id from,
                           IndexedFunction<? super T, ? extends A> map,
                           BiFunction<? super A, ? super A, ? extends A> reduce,
                           A zero,
                           Predicate<A> isTerminal)
    {
        return mapReduce(0, topologies.size(), from, map, reduce, zero, isTerminal);
    }

    public <A> A mapReduce(int topologyStart, int topologyEnd,
                           Node.Id from,
                           IndexedFunction<? super T, ? extends A> map,
                           BiFunction<? super A, ? super A, ? extends A> reduce,
                           A zero,
                           Predicate<A> isTerminal)
    {
        A accum = zero;
        for (int i = topologyStart; i < topologyEnd && !isTerminal.test(accum); i++)
        {
            Topology t = topologies.get(i);
            int offset = offsets[i];
            accum = t.<Void, Void, Void, A>mapReduceOn(from, offset, (p1, p2, p3, index) -> map.apply(array[index], index), null, null, null, reduce, accum);
        }
        return accum;
    }
}
