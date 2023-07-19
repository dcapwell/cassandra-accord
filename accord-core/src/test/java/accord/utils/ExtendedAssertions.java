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

package accord.utils;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ShouldBeEmpty;
import org.assertj.core.error.ShouldHaveSize;
import org.assertj.core.error.ShouldNotBeEmpty;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class ExtendedAssertions
{
    public static class ShardAssert extends AbstractAssert<ShardAssert, Shard>
    {
        protected ShardAssert(Shard shard)
        {
            super(shard, ShardAssert.class);
        }

        public ShardAssert contains(RoutingKey key)
        {
            isNotNull();
            if (!actual.contains(key))
                throwAssertionError(new BasicErrorMessageFactory("%nExpected shard to contain key %s, but did not; range was %s", key, actual.range));
            return myself;
        }
    }

    public static class TopologyAssert extends AbstractAssert<TopologyAssert, Topology>
    {
        protected TopologyAssert(Topology topology)
        {
            super(topology, TopologyAssert.class);
        }

        public TopologyAssert isSubset()
        {
            isNotNull();
            if (!actual.isSubset())
                throwAssertionError(new BasicErrorMessageFactory("%nExpected to be a subset but was not"));
            return myself;
        }

        public TopologyAssert isNotSubset()
        {
            isNotNull();
            if (actual.isSubset())
                throwAssertionError(new BasicErrorMessageFactory("%nExpected not to be a subset but was"));
            return myself;
        }

        public TopologyAssert hasSize(int size)
        {
            isNotNull();
            if (actual.size() != size)
                throwAssertionError(ShouldHaveSize.shouldHaveSize(actual, actual.size(), size));
            return myself;
        }

        public TopologyAssert isShardsEqualTo(Shard... shards)
        {
            return isShardsEqualTo(Arrays.asList(shards));
        }

        public TopologyAssert isShardsEqualTo(List<Shard> shards)
        {
            isNotNull();
            objects.assertEqual(info, actual.shards(), shards);
            return myself;
        }

        public TopologyAssert isHostsEqualTo(List<Node.Id> nodes)
        {
            isNotNull();
            objects.assertEqual(info, actual.nodes(), new HashSet<>(nodes));
            return myself;
        }

        public TopologyAssert isRangesEqualTo(Range... range)
        {
            return isRangesEqualTo(Ranges.of(range));
        }

        public TopologyAssert isRangesEqualTo(Ranges ranges)
        {
            isNotNull();
            objects.assertEqual(info, actual.ranges(), ranges);
            return myself;
        }
    }

    public static class TopologiesAssert extends AbstractAssert<TopologiesAssert, Topologies>
    {
        protected TopologiesAssert(Topologies topologies)
        {
            super(topologies, TopologiesAssert.class);
        }

        public TopologiesAssert isEmpty()
        {
            isNotNull();
            if (!actual.isEmpty())
                throwAssertionError(ShouldBeEmpty.shouldBeEmpty(actual));
            return myself;
        }

        public TopologiesAssert isNotEmpty()
        {
            isNotNull();
            if (actual.isEmpty())
                throwAssertionError(ShouldNotBeEmpty.shouldNotBeEmpty());
            return myself;
        }

        public TopologiesAssert hasEpochsBetween(long min, long max)
        {
            isNotNull();
            if (!(actual.oldestEpoch() >= min && actual.oldestEpoch() <= max && actual.currentEpoch() >= min && actual.currentEpoch() <= max))
                throwAssertionError(new BasicErrorMessageFactory("%nExpected epochs between %d and %d, but given %d and %d", min, max, actual.oldestEpoch(), actual.currentEpoch()));
            return myself;
        }

        public TopologiesAssert containsAll(Unseekables<?> select)
        {
            isNotNull();
            for (int i = 0; i < actual.size(); i++)
            {
                Topology topology = actual.get(i);
                select = select.subtract(topology.ranges());
                if (select.isEmpty()) return myself;
            }
            throwAssertionError(new BasicErrorMessageFactory("%nMissing ranges detected: %s", select));
            return myself;
        }

        public TopologiesAssert containsAll(TopologyManager service, Unseekables<?> select)
        {
            isNotNull();
            for (int i = 0; i < actual.size(); i++)
            {
                Topology selected = actual.get(i);
                Topology global = service.globalForEpoch(selected.epoch());
                Unseekables<?> globalIntersect = select.slice(global.ranges());
                Unseekables<?> selectedIntersect = select.slice(selected.ranges());
                if (!globalIntersect.equals(selectedIntersect))
                    throwAssertionError(new BasicErrorMessageFactory("%nEpoch %s: select %s expected to match %s but actually matched %s", global.epoch(), select, globalIntersect, selectedIntersect));
            }
            return myself;
        }
    }

    public static ShardAssert assertThat(Shard shard)
    {
        return new ShardAssert(shard);
    }

    public static TopologyAssert assertThat(Topology topology)
    {
        return new TopologyAssert(topology);
    }

    public static TopologiesAssert assertThat(Topologies topologies)
    {
        return new TopologiesAssert(topologies);
    }
}
