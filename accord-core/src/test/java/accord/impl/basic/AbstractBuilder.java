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

package accord.impl.basic;

import accord.api.Agent;
import accord.api.ConfigurationService;
import accord.api.DataStore;
import accord.api.MessageSink;
import accord.api.ProgressLog;
import accord.api.Scheduler;
import accord.api.TopologySorter;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;
import accord.utils.ThreadPoolScheduler;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class AbstractBuilder<T extends AbstractBuilder<T>>
{
    MessageSink messageSink = Mockito.mock(MessageSink.class);
    ConfigurationService configService;
    LongSupplier nowSupplier = new MockCluster.Clock(100);
    Supplier<DataStore> dataSupplier;
    {
        MockStore store = new MockStore();
        dataSupplier = () -> store;
    }
    ShardDistributor shardDistributor = new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter());
    Agent agent = new TestAgent();
    RandomSource random = new DefaultRandom(42);
    Scheduler scheduler = new ThreadPoolScheduler();
    TopologySorter.Supplier topologySorter = SizeOfIntersectionSorter.SUPPLIER;
    Function<Node, ProgressLog.Factory> progressLogFactory = SimpleProgressLog::new;
    // TODO (thread safety, now): Synchronized.maybeRun can deadlock when node.mapReduceLocal is called
    CommandStores.Factory factory = InMemoryCommandStores.Synchronized::new;
    List<Topology> topologies = Collections.emptyList();

    public AbstractBuilder()
    {
    }

    public AbstractBuilder(AbstractBuilder<?> other)
    {
        this.messageSink = other.messageSink;
        this.configService = other.configService;
        this.nowSupplier = other.nowSupplier;;
        this.dataSupplier = other.dataSupplier;;
        this.shardDistributor = other.shardDistributor;
        this.agent = other.agent;
        this.random = other.random;
        this.scheduler = other.scheduler;
        this.topologySorter = other.topologySorter;
        this.progressLogFactory = other.progressLogFactory;
        this.factory = other.factory;
        this.topologies = other.topologies;
    }

    public T withAgent(Agent agent)
    {
        this.agent = agent;
        return (T) this;
    }

    public T withDataSupplier(Supplier<DataStore> dataSupplier)
    {
        this.dataSupplier = dataSupplier;
        return (T) this;
    }

    public T withKeyType(KeyType type)
    {
        withShardDistributorFromSplitter(type.splitter);
        return (T) this;
    }

    public T withTopologySorter(TopologySorter.Supplier s)
    {
        this.topologySorter = s;
        return (T) this;
    }

    public T withSink(MessageSink sink)
    {
        this.messageSink = sink;
        return (T) this;
    }

    public T withTopologies(Topology... topologies)
    {
        this.topologies = Arrays.asList(topologies);
        this.topologies.sort(Comparator.comparingLong(Topology::epoch));
        return (T) this;
    }

    public T withProgressLog(ProgressLog.Factory factory)
    {
        return withProgressLog((Function<Node, ProgressLog.Factory>) ignore -> factory);
    }

    public T withProgressLog(Function<Node, ProgressLog.Factory> fn)
    {
        this.progressLogFactory = fn;
        return (T) this;
    }

    public T withMessageSink(MessageSink messageSink)
    {
        this.messageSink = Objects.requireNonNull(messageSink);
        return (T) this;
    }

    public T withClock(LongSupplier clock)
    {
        this.nowSupplier = clock;
        return (T) this;
    }

    public T withShardDistributor(ShardDistributor shardDistributor)
    {
        this.shardDistributor = shardDistributor;
        return (T) this;
    }

    public T withShardDistributorFromSplitter(Function<Ranges, ? extends ShardDistributor.EvenSplit.Splitter<?>> splitter)
    {
        return withShardDistributor(new ShardDistributor.EvenSplit(8, splitter));
    }

}
