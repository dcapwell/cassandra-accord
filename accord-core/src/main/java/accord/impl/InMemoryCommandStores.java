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

package accord.impl;

import accord.local.*;
import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.primitives.Routables;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncChains;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;


public abstract class InMemoryCommandStores<S extends InMemoryCommandStore> extends CommandStores<S>
{
    public InMemoryCommandStores(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        super(time, agent, store, shardDistributor, progressLogFactory, shardFactory);
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume, CommandStores.AsyncMapReduceAdapter.instance());
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, commandStoreIds, mapReduceConsume, CommandStores.AsyncMapReduceAdapter.instance());
    }

    public static class Synchronized extends InMemoryCommandStores<InMemoryCommandStore.Synchronized>
    {
        public Synchronized(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory)
        {
            super(time, agent, store, shardDistributor, progressLogFactory, InMemoryCommandStore.Synchronized::new);
        }

        public <T> T mapReduce(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, T> map)
        {
            return AsyncChains.getUninterruptibly(super.mapReduce(context, keys, minEpoch, maxEpoch, map, AsyncMapReduceAdapter.instance()));
        }

        public <T> T mapReduce(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, Function<? super SafeCommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(context, keys, minEpoch, maxEpoch, new MapReduce<SafeCommandStore, T>() {
                @Override
                public T apply(SafeCommandStore in)
                {
                    return map.apply(in);
                }

                @Override
                public T reduce(T o1, T o2)
                {
                    return reduce.apply(o1, o2);
                }
            });
        }
    }

    public static class SingleThread extends InMemoryCommandStores<InMemoryCommandStore.SingleThread>
    {
        public SingleThread(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory)
        {
            super(time, agent, store, shardDistributor, progressLogFactory, InMemoryCommandStore.SingleThread::new);
        }

        public SingleThread(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            super(time, agent, store, shardDistributor, progressLogFactory, shardFactory);
        }
    }

    public static class Debug extends InMemoryCommandStores.SingleThread
    {
        public Debug(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory)
        {
            super(time, agent, store, shardDistributor, progressLogFactory, InMemoryCommandStore.Debug::new);
        }
    }
}
