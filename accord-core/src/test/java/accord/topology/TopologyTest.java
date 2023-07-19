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

import accord.Utils;
import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TopologyFactory;
import accord.local.Node;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.utils.Gens;
import accord.utils.RandomSource;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static accord.utils.AccordGens.topologys;
import static org.assertj.core.api.Assertions.assertThat;
import static accord.utils.ExtendedAssertions.assertThat;
import static accord.utils.Property.qt;

public class TopologyTest
{
    private static void assertRangeForKey(Topology topology, int key, int start, int end)
    {
        Key expectedKey = IntKey.key(key);
        Shard shard = topology.forKey(IntKey.routing(key));
        Range expectedRange = IntKey.range(start, end);
        Assertions.assertTrue(expectedRange.contains(expectedKey));
        Assertions.assertTrue(shard.range.contains(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);

        Topology subTopology = topology.forSelection(Keys.of(expectedKey).toParticipants());
        shard = Iterables.getOnlyElement(subTopology.shards());
        Assertions.assertTrue(shard.range.contains(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);
    }

    private static Topology topology(List<Node.Id> ids, int rf, Range... ranges)
    {
        TopologyFactory topologyFactory = new TopologyFactory(rf, ranges);
        return topologyFactory.toTopology(ids);
    }

    private static Topology topology(int numNodes, int rf, Range... ranges)
    {
        return topology(Utils.ids(numNodes), rf, ranges);
    }

    private static Topology topology(Range... ranges)
    {
        return topology(1, 1, ranges);
    }

    private static Range r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static void assertNoRangeForKey(Topology topology, int key)
    {
        try
        {
            topology.forKey(IntKey.routing(key));
            Assertions.fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // noop
        }
    }

    @Test
    void forKeyTest()
    {
        Topology topology = topology(r(0, 100), r(100, 200), r(300, 400));
        assertNoRangeForKey(topology, -50);
        assertRangeForKey(topology, 50, 0, 100);
        assertRangeForKey(topology, 100, 0, 100);
        assertNoRangeForKey(topology, 250);
        assertRangeForKey(topology, 350, 300, 400);
    }

    @Test
    void forRangesTest()
    {

    }

    @Test
    void basic()
    {
        qt().withSeed(247220790093642898L).forAll(topologys(), Gens.random()).check((topology, rs) -> {
            Ranges ranges = topology.ranges;
            assertThat(topology)
                    .isNotSubset()
                    .isEqualTo(topology.withEpoch(topology.epoch))
                    .hasSameHashCodeAs(topology.withEpoch(topology.epoch));

            checkTopology(topology, rs);

            for (int i = 0; i < topology.size(); i++)
            {
                Shard shard = topology.get(i);
                Topology subset = topology.forSubset(new int[] {i});
                Topology trimmed = subset.trim();

                assertThat(subset)
                        .isSubset()
                        .isEqualTo(trimmed)
                        .hasSameHashCodeAs(trimmed)
                        // this is slightly redundant as trimmed model should catch this... it is here in case trim breaks
                        .hasSize(1)
                        .isShardsEqualTo(shard)
                        .isHostsEqualTo(shard.nodes)
                        .isRangesEqualTo(shard.range);

                checkTopology(subset, rs);
                {
                    List<Shard> forEachShard = new ArrayList<>(1);
                    subset.forEach(s -> forEachShard.add(s)); // cant do forEachShard::add due ambiguous signature (multiple matches in topology)
                    assertThat(forEachShard).isEqualTo(Arrays.asList(shard));
                }

                for (Range range : subset.ranges())
                {
                    RoutingKey key = routing(range, rs);
                    assertThat(subset.forKey(key)).isEqualTo(shard);
                }

                for (Node.Id node : new TreeSet<>(subset.nodes()))
                {
                    assertThat(subset.forNode(node))
                            .isEqualTo(trimmed.forNode(node))
                            .isRangesEqualTo(subset.rangesForNode(node))
                            .isRangesEqualTo(trimmed.rangesForNode(node));
                }

                // TODO
                // by Node
                // public <P> int foldlIntOn(Id on, IndexedIntFunction<P> consumer, P param, int offset, int initialValue, int terminalValue)
                // public <P1, P2, P3, O> O mapReduceOn(Id on, int offset, IndexedTriFunction<? super P1, ? super P2, ? super P3, ? extends O> function, P1 p1, P2 p2, P3 p3, BiFunction<? super O, ? super O, ? extends O> reduce, O initialValue)

                // by Range
                // public <T> T foldl(Unseekables<?> select, IndexedBiFunction<Shard, T, T> function, T accumulator)
                // public void visitNodeForKeysOnceOrMore(Unseekables<?> select, Consumer<Id> nodes)
                // public Topology forSelection(Unseekables<?> select, Collection<Id> nodes)
                // public Topology forSelection(Unseekables<?> select)
            }
        });
    }

    private static void checkTopology(Topology topology, RandomSource rs)
    {
        for (Node.Id node : topology.nodes())
            assertThat(topology.forNode(node)).isRangesEqualTo(topology.rangesForNode(node));
        for (Range range : topology.ranges())
        {
            for (int i = 0; i < 10; i++)
            {
                RoutingKey key = routing(range, rs);

                assertThat(topology.forKey(key))
                        .describedAs("forKey(key) != get(indexForKey(key)) for key %s", key)
                        .isEqualTo(topology.get(topology.indexForKey(key)))
                        .contains(key);
            }
        }
    }

    private static RoutingKey routing(Ranges ranges, RandomSource rs)
    {
        Range range = ranges.get(rs.nextInt(ranges.size()));
        return routing(range, rs);
    }

    private static RoutingKey routing(Range range, RandomSource rs)
    {
        if (range.start() instanceof PrefixedIntHashKey)
        {
            PrefixedIntHashKey.Hash start = (PrefixedIntHashKey.Hash) range.start();
            PrefixedIntHashKey.Hash end = (PrefixedIntHashKey.Hash) range.end();
            int value = rs.nextInt(start.hash, end.hash);
            if (range.endInclusive()) // exclude start, but include end... so +1
                value++;
            return PrefixedIntHashKey.forHash(start.prefix, value);
        }
        else
        {
            throw new IllegalArgumentException("Key type " + range.start().getClass() + " is not supported");
        }
    }
}
