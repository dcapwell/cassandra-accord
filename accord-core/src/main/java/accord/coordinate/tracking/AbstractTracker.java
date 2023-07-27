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

package accord.coordinate.tracking;

import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.ShardsArray;

import accord.utils.Invariants;
import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;

public abstract class AbstractTracker<ST extends ShardTracker>
{
    public enum ShardOutcomes implements ShardOutcome<AbstractTracker<?>>
    {
        Fail(RequestStatus.Failed),
        Success(RequestStatus.Success),
        SendMore(null),
        NoChange(RequestStatus.NoChange);

        final RequestStatus result;

        ShardOutcomes(RequestStatus result) {
            this.result = result;
        }

        private boolean isTerminalState()
        {
            return compareTo(Success) <= 0;
        }

        private static ShardOutcomes min(ShardOutcomes a, ShardOutcomes b)
        {
            return a.compareTo(b) <= 0 ? a : b;
        }

        @Override
        public ShardOutcomes apply(AbstractTracker<?> tracker, int shardIndex)
        {
            if (this == Success)
                return --tracker.waitingOnShards == 0 ? Success : NoChange;
            return this;
        }

        private RequestStatus toRequestStatus(AbstractTracker<?> tracker)
        {
            if (result != null)
                return result;
            return tracker.trySendMore();
        }
    }

    public interface ShardFactory<ST extends ShardTracker>
    {
        ST apply(int epochIndex, Shard shard);
    }

    final Topologies topologies;
    protected final ShardsArray<ST> trackers;
    protected final int maxShardsPerEpoch;
    protected int waitingOnShards;

    AbstractTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, Function<Shard, ST> trackerFactory)
    {
        this(topologies, arrayFactory, (ignore, shard) -> trackerFactory.apply(shard));
    }
    AbstractTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, ShardFactory<ST> trackerFactory)
    {
        Invariants.checkArgument(topologies.totalShards() > 0);
        this.trackers = new ShardsArray(topologies, arrayFactory, (epochIndex, ignore, shard) -> trackerFactory.apply(epochIndex, shard));
        this.topologies = topologies;
        this.maxShardsPerEpoch = IntStream.range(0, topologies.size()).map(i -> topologies.get(i).size()).max().orElse(0);
        this.waitingOnShards = trackers.size();
    }

    public Topologies topologies()
    {
        return topologies;
    }

    protected RequestStatus trySendMore() { throw new UnsupportedOperationException(); }

    <T extends AbstractTracker<ST>, P>
    RequestStatus recordResponse(T self, Id node, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param)
    {
        return recordResponse(self, node, function, param, topologies.size());
    }

    <T extends AbstractTracker<ST>, P>
    RequestStatus recordResponse(T self, Id node, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param, int topologyLimit)
    {
        Invariants.checkState(self == this); // we just accept self as parameter for type safety
        ShardOutcomes status = NoChange;
        status = trackers.mapReduce(0, topologyLimit, node,
                                    (value, index) -> function.apply(value, param).apply(self, index),
                                    ShardOutcomes::min,
                                    status,
                                    ShardOutcomes::isTerminalState);

        return status.toRequestStatus(this);
    }

    public boolean any(Predicate<ST> test)
    {
        for (ST tracker : trackers)
        {
            if (test.test(tracker))
                return true;
        }
        return false;
    }

    public boolean all(Predicate<ST> test)
    {
        for (ST tracker : trackers)
        {
            if (!test.test(tracker))
                return false;
        }
        return true;
    }

    public Set<Id> nodes()
    {
        return topologies.nodes();
    }

    public ST get(int shardIndex)
    {
        return trackers.get(shardIndex);
    }

    @VisibleForTesting
    public ST get(int topologyIdx, int shardIdx)
    {
        return trackers.get(topologyIdx, shardIdx);
    }

    protected int maxShardsPerEpoch()
    {
        return maxShardsPerEpoch;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "trackers=" + trackers +
               '}';
    }
}
