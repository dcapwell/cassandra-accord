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

import accord.burn.TopologyUpdates;
import accord.coordinate.CoordinateGloballyDurable;
import accord.coordinate.CoordinateShardDurable;
import accord.coordinate.CoordinateSyncPoint;
import accord.primitives.SyncPoint;
import accord.utils.RandomSource;
import accord.impl.PrefixedIntHashKey;
import accord.impl.PrefixedIntHashKey.Hash;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.utils.Invariants;

import com.google.common.collect.Sets;
import org.agrona.collections.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;


// TODO (required, testing): add change replication factor
public class TopologyRandomizer
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyRandomizer.class);

    private static final AtomicInteger CURRENT_PREFIX = new AtomicInteger(0);

    private final RandomSource random;
    private final List<Topology> epochs = new ArrayList<>();
    private final Function<Node.Id, Node> nodeLookup;
    private final Map<Node.Id, Ranges> previouslyReplicated = new HashMap<>();
    private final TopologyUpdates topologyUpdates;

    public TopologyRandomizer(Supplier<RandomSource> randomSupplier, Topology initialTopology, TopologyUpdates topologyUpdates, @Nullable Function<Node.Id, Node> nodeLookup)
    {
        this.random = randomSupplier.get();
        this.topologyUpdates = topologyUpdates;
        this.epochs.add(Topology.EMPTY);
        this.epochs.add(initialTopology);
        for (Node.Id node : initialTopology.nodes())
            previouslyReplicated.put(node, initialTopology.rangesForNode(node));
        this.nodeLookup = nodeLookup;
    }

    public enum UpdateType
    {
//        BOUNDARY(TopologyRandomizer::updateBoundary),
        SPLIT(TopologyRandomizer::split),
        MERGE(TopologyRandomizer::merge),
        MEMBERSHIP(TopologyRandomizer::updateMembership),
        FASTPATH(TopologyRandomizer::updateFastPath),
        ADD_PREFIX(TopologyRandomizer::addPrefix);
//        REMOVE_PREFIX(TopologyRandomizer::removePrefix);

        private final BiFunction<Shard[], RandomSource, Shard[]> function;

        UpdateType(BiFunction<Shard[], RandomSource, Shard[]> function)
        {
            this.function = function;
        }

        public Shard[] apply(Shard[] shards, RandomSource random)
        {
            return function.apply(shards, random);
        }

        static UpdateType kind(RandomSource random)
        {
            int idx = random.nextInt(values().length);
            return values()[idx];
        }
    }

    private static Shard[] updateBoundary(Shard[] shards, RandomSource random)
    {
        // TODO (now, correctness): left/right may have a diff prefix
        int idx = random.nextInt(shards.length - 1);
        Shard left = shards[idx];
        PrefixedIntHashKey.Range leftRange = (PrefixedIntHashKey.Range) left.range;
        Shard right = shards[idx + 1];
        PrefixedIntHashKey.Range rightRange = (PrefixedIntHashKey.Range) right.range;
        PrefixedIntHashKey minBound = (PrefixedIntHashKey) leftRange.split(2).get(0).end();
        PrefixedIntHashKey maxBound = (PrefixedIntHashKey) rightRange.split(2).get(0).start();

        if (minBound.hash == maxBound.hash)
            // no adjustment is possible
            return shards;

        Hash newBound = PrefixedIntHashKey.forHash(minBound.prefix, minBound.hash + random.nextInt(maxBound.hash - minBound.hash));

        shards[idx] = new Shard(PrefixedIntHashKey.range((Hash)leftRange.start(), newBound), left.nodes, left.fastPathElectorate, left.joining);
        shards[idx+1] = new Shard(PrefixedIntHashKey.range(newBound, (Hash)rightRange.end()), right.nodes, right.fastPathElectorate, right.joining);
//        logger.debug("Updated boundary on {} & {} {} {} to {} {}", idx, idx + 1, left, right,
//                     shards[idx].toString(true), shards[idx + 1].toString(true));

        return shards;
    }

    private static Shard[] split(Shard[] shards, RandomSource random)
    {
        if (shards.length == 0)
            throw new IllegalArgumentException("Unable to split an empty array");
        int idx = shards.length == 1 ? 0 : random.nextInt(shards.length - 1);
        Shard split = shards[idx];
        PrefixedIntHashKey.Range splitRange = (PrefixedIntHashKey.Range) split.range;
        PrefixedIntHashKey minBound = (PrefixedIntHashKey) splitRange.start();
        PrefixedIntHashKey maxBound = (PrefixedIntHashKey) splitRange.end();

        if (minBound.hash + 1 == maxBound.hash)
            // no split is possible
            return shards;

        Hash newBound = PrefixedIntHashKey.forHash(minBound.prefix, minBound.hash + 1 + random.nextInt(maxBound.hash - (1 + minBound.hash)));

        Shard[] result = new Shard[shards.length + 1];
        System.arraycopy(shards, 0, result, 0, idx);
        System.arraycopy(shards, idx, result, idx + 1, shards.length - idx);
        result[idx] = new Shard(PrefixedIntHashKey.range((Hash)splitRange.start(), newBound), split.nodes, split.fastPathElectorate, split.joining);
        result[idx+1] = new Shard(PrefixedIntHashKey.range(newBound, (Hash)splitRange.end()), split.nodes, split.fastPathElectorate, split.joining);
        logger.debug("Split boundary on {} & {} {} to {} {}", idx, idx + 1, split,
                     result[idx].toString(true), result[idx + 1].toString(true));

        return result;
    }

    private static Shard[] merge(Shard[] shards, RandomSource random)
    {
        if (shards.length <= 1)
            return shards;

        int idx = shards.length == 2 ? 0 : random.nextInt(shards.length - 2);
        Shard left = shards[idx];
        Shard right = shards[idx + 1];
        while (prefix(left) != prefix(right))
        {
            // shards are a single prefix, so can't merge
            if (idx + 2 == shards.length)
                return shards;
            idx++;
            left = shards[idx];
            right = shards[idx + 1];
        }

        Shard[] result = new Shard[shards.length - 1];
        System.arraycopy(shards, 0, result, 0, idx);
        System.arraycopy(shards, idx + 2, result, idx + 1, shards.length - (idx + 2));
        Range range = PrefixedIntHashKey.range((Hash)left.range.start(), (Hash)right.range.end());
        List<Node.Id> nodes; {
            TreeSet<Node.Id> tmp = new TreeSet<>();
            tmp.addAll(left.nodes);
            tmp.addAll(right.nodes);
            nodes = new ArrayList<>(tmp);
        }
        Set<Node.Id> joining = new TreeSet<>();
        joining.addAll(left.joining);
        joining.addAll(right.joining);
        result[idx] = new Shard(range, nodes, newFastPath(nodes, random), joining);
        logger.debug("Merging at {} & {} {} {} to {}", idx, idx + 1, left, right,
                     shards[idx].toString(true));
        return result;
    }

    private static Shard[] updateMembership(Shard[] shards, RandomSource random)
    {
        if (shards.length <= 1)
            return shards;

        int idxLeft = random.nextInt(shards.length);
        Shard shardLeft = shards[idxLeft];

        // bail out if all shards have the same membership
        if (Arrays.stream(shards).allMatch(shard -> shard.sortedNodes.containsAll(shardLeft.sortedNodes) || shardLeft.containsAll(shard.sortedNodes)))
            return shards;

        Set<Node.Id> joining = new HashSet<>();
        joining.addAll(shardLeft.joining);

        int idxRight;
        Shard shardRight;
        do {
            idxRight = random.nextInt(shards.length);
            shardRight = shards[idxRight];
            joining.addAll(shardRight.joining);
        } while (idxRight == idxLeft || shardLeft.sortedNodes.containsAll(shardRight.sortedNodes) || shardRight.sortedNodes.containsAll(shardLeft.sortedNodes));

        List<Node.Id> nodesLeft;
        Node.Id toRight;
        for (;;)
        {
            nodesLeft = new ArrayList<>(shardLeft.nodes);
            toRight = nodesLeft.remove(random.nextInt(nodesLeft.size()));
            if (!shardRight.contains(toRight))
                break;
        }

        List<Node.Id> nodesRight;
        Node.Id toLeft;
        for (;;)
        {
            nodesRight = new ArrayList<>(shardRight.nodes);
            toLeft = nodesRight.remove(random.nextInt(nodesRight.size()));
            if (!nodesLeft.contains(toLeft))
                break;
        }

        nodesLeft.add(toLeft);
        nodesRight.add(toRight);

        Shard[] newShards = shards.clone();
        newShards[idxLeft] = new Shard(shardLeft.range, nodesLeft, newFastPath(nodesLeft, random), Sets.intersection(joining, new HashSet<>(nodesLeft)));
        newShards[idxRight] = new Shard(shardRight.range, nodesRight, newFastPath(nodesRight, random), Sets.intersection(joining, new HashSet<>(nodesRight)));
        logger.debug("updated membership on {} & {} {} {} to {} {}",
                    idxLeft, idxRight,
                    shardLeft.toString(true), shardRight.toString(true),
                    newShards[idxLeft].toString(true), newShards[idxRight].toString(true));

        return newShards;
    }

    private static Set<Node.Id> newFastPath(List<Node.Id> nodes, RandomSource random)
    {
        List<Node.Id> available = new ArrayList<>(nodes);
        int rf = available.size();
        int f = Shard.maxToleratedFailures(rf);
        int minSize = rf - f;
        int newSize = minSize + random.nextInt(f + 1);

        Set<Node.Id> fastPath = new HashSet<>();
        for (int i=0; i<newSize; i++)
        {
            int idx = random.nextInt(available.size());
            fastPath.add(available.remove(idx));
        }

        return fastPath;
    }

    private static Shard[] updateFastPath(Shard[] shards, RandomSource random)
    {
        int idx = random.nextInt(shards.length);
        Shard shard = shards[idx];
        shards[idx] = new Shard(shard.range, shard.nodes, newFastPath(shard.nodes, random), shard.joining);
//        logger.debug("Updated fast path on {} {} to {}", idx, shard.toString(true), shards[idx].toString(true));
        return shards;
    }

    private static Shard[] addPrefix(Shard[] shards, RandomSource random)
    {
        // TODO (coverage): add support for bringing prefixes back after removal
        // In implementations (such as Apache Cassandra) its possible that a range exists, gets removed, then added back (CREATE KEYSPACE, DROP KEYSPACE, CREATE KEYSPACE),
        // in this case the old prefix should be "cleared".
        int prefix = CURRENT_PREFIX.incrementAndGet();
        Set<Node.Id> joining = new HashSet<>();
        Node.Id[] nodes;
        {
            Set<Node.Id> uniq = new HashSet<>();
            for (Shard shard : shards)
            {
                uniq.addAll(shard.nodes);
                joining.addAll(shard.joining);
            }
            Node.Id[] result = uniq.toArray(new Node.Id[uniq.size()]);
            Arrays.sort(result);
            nodes = result;
        }
        int rf = Math.min(3, nodes.length);
        List<Shard> result = new ArrayList<>(shards.length + nodes.length);
        result.addAll(Arrays.asList(shards));
        Range[] ranges = PrefixedIntHashKey.ranges(prefix, nodes.length);
        for (int i = 0; i < ranges.length; i++)
        {
            Range range = ranges[i];
            List<Node.Id> replicas = select(nodes, rf, random);
            Set<Node.Id> fastPath = newFastPath(replicas, random);
            result.add(new Shard(range, replicas, fastPath, Sets.intersection(joining, new HashSet<>(replicas))));
        }
        return result.toArray(new Shard[result.size()]);
    }

    private static List<Node.Id> select(Node.Id[] nodes, int rf, RandomSource random)
    {
        List<Node.Id> result = new ArrayList<>(rf);
        while (result.size() < rf)
        {
            Node.Id id = nodes[random.nextInt(nodes.length)];
            // TODO (efficiency) : rf is normally 3, so is it worth it to have a set, bitset, or another structure?
            if (!result.contains(id))
                result.add(id);
        }
        return result;
    }

    private static Shard[] removePrefix(Shard[] shards, RandomSource random)
    {
        int[] prefixes = prefixes(shards);
        if (prefixes.length <= 1)
            return shards;
        int prefix = prefixes[random.nextInt(prefixes.length)];
        List<Shard> result = new ArrayList<>(shards.length);
        for (Shard shard : shards)
        {
            int shardPrefix = prefix(shard);
            if (shardPrefix == prefix) continue;
            result.add(shard);
        }
        return result.toArray(result.toArray(new Shard[result.size()]));
    }

    private static int[] prefixes(Shard[] shards)
    {
        IntHashSet prefixes = new IntHashSet();
        for (Shard shard : shards)
            prefixes.add(prefix(shard));
        int[] result = new int[prefixes.size()];
        IntHashSet.IntIterator it = prefixes.iterator();
        for (int i = 0; it.hasNext(); i++)
            result[i] = it.nextValue();
        Arrays.sort(result);
        return result;
    }

    private static int prefix(Shard shard)
    {
        return ((PrefixedIntHashKey) shard.range.start()).prefix;
    }

    private static Map<Node.Id, Ranges> getAdditions(Topology current, Topology next)
    {
        Map<Node.Id, Ranges> additions = new HashMap<>();
        for (Node.Id node : next.nodes())
        {
            Ranges prev = current.rangesForNode(node);
            if (prev == null) prev = Ranges.EMPTY;

            Ranges added = next.rangesForNode(node).subtract(prev);
            if (added.isEmpty())
                continue;

            additions.put(node, added);
        }
        return additions;
    }

    private static boolean reassignsRanges(Topology current, Shard[] nextShards, Map<Node.Id, Ranges> previouslyReplicated)
    {
        Topology next = new Topology(current.epoch + 1, nextShards);
        Map<Node.Id, Ranges> additions = getAdditions(current, next);

        for (Map.Entry<Node.Id, Ranges> entry : additions.entrySet())
        {
            if (previouslyReplicated.getOrDefault(entry.getKey(), Ranges.EMPTY).intersects(entry.getValue()))
                return true;
        }
        return false;
    }

    public synchronized void maybeUpdateTopology()
    {
        // if we don't limit the number of pending topology changes in flight,
        // the topology randomizer will keep the burn test busy indefinitely
        if (topologyUpdates.pendingTopologies() > 5)
            return;

        updateTopology();
    }

    public synchronized void rangeDurable()
    {
        Invariants.nonNull(nodeLookup);
        Topology current = epochs.get(epochs.size() - 1);
        Node node = nodeLookup.apply(random.pick(current.nodes()));
        Ranges ranges = selectRanges(node.topology().currentLocal().ranges);
        CoordinateSyncPoint.exclusive(node, ranges)
                           .addCallback((success, fail) -> {
                            if (success != null)
                                coordinateDurable(node, success);
        });
    }

    private static void coordinateDurable(Node node, SyncPoint exclusiveSyncPoint)
    {
        CoordinateShardDurable.coordinate(node, exclusiveSyncPoint)
                              .addCallback((success0, fail0) -> {
                                  if (fail0 != null) coordinateDurable(node, exclusiveSyncPoint);
                              });
    }

    public synchronized void globallyDurable()
    {
        Invariants.nonNull(nodeLookup);
        Topology current = epochs.get(epochs.size() - 1);
        List<Node.Id> nodes = new ArrayList<>(current.nodes());
        Node.Id nodeId = nodes.get(random.nextInt(nodes.size()));
        Node node = nodeLookup.apply(nodeId);
        long epoch = current.epoch == 1 ? 1 : 1 + random.nextInt((int)current.epoch - 1);
        node.withEpoch(epoch, () -> {
            node.commandStores().any().execute(() -> CoordinateGloballyDurable.coordinate(node, epoch));
        });
    }

    private Ranges selectRanges(Ranges ranges)
    {
        if (random.decide(0.1f))
            return ranges;

        int count = ranges.size() == 1 ? 1 : 1 + random.nextInt(Math.min(3, ranges.size() - 1));
        Range[] out = new Range[count];
        while (count-- > 0)
        {
            int idx = random.nextInt(ranges.size());
            out[count] = subRange(ranges.get(idx));
        }
        return Ranges.of(out);
    }

    private Range subRange(Range range)
    {
        PrefixedIntHashKey minBound = (PrefixedIntHashKey) range.start();
        PrefixedIntHashKey maxBound = (PrefixedIntHashKey) range.end();
        if (minBound.hash + 3 >= maxBound.hash)
            return range;

        Hash lb = PrefixedIntHashKey.forHash(minBound.prefix, minBound.hash + 1 + random.nextInt(maxBound.hash - (2 + minBound.hash)));
        int length = Math.min(1 + (int)((0.05f + (0.1f * random.nextFloat())) * (maxBound.hash - (1 + minBound.hash))), maxBound.hash - lb.hash);
        Hash ub = PrefixedIntHashKey.forHash(minBound.prefix, lb.hash + length);
        return range.newRange(lb, ub);
    }

    public synchronized Topology updateTopology()
    {
        Topology current = epochs.get(epochs.size() - 1);
        Shard[] oldShards = current.unsafeGetShards().clone();
        int remainingMutations = random.nextInt(current.size());
        int rejectedMutations = 0;
        logger.debug("Updating topology with {} mutations", remainingMutations);
        Shard[] newShards = oldShards;
        while (remainingMutations > 0 && rejectedMutations < 10)
        {
            UpdateType type = UpdateType.kind(random);
            Shard[] testShards = type.apply(newShards, random);
            if (!everyShardHasOverlaps(current.epoch, oldShards, testShards)
                // TODO (now): I don't think it is necessary to prevent re-replicating ranges any longer
                || reassignsRanges(current, testShards, previouslyReplicated)
            )
            {
                ++rejectedMutations;
            }
            else
            {
                newShards = testShards;
                --remainingMutations;
            }
        }

        if (newShards == oldShards)
            return null;

        Topology nextTopology = new Topology(current.epoch + 1, newShards);

        Map<Node.Id, Ranges> nextAdditions = getAdditions(current, nextTopology);
        for (Map.Entry<Node.Id, Ranges> entry : nextAdditions.entrySet())
        {
            Ranges previous = previouslyReplicated.getOrDefault(entry.getKey(), Ranges.EMPTY);
            Ranges added = entry.getValue();
            Ranges merged = previous.with(added).mergeTouching();
            previouslyReplicated.put(entry.getKey(), merged);
        }

//        logger.debug("topology update to: {} from: {}", nextTopology, current);
        epochs.add(nextTopology);

        if (nodeLookup != null)
        {
            List<Node.Id> nodes = new ArrayList<>(nextTopology.nodes());
            int originatorIdx = random.nextInt(nodes.size());
            topologyUpdates.notify(nodeLookup.apply(nodes.get(originatorIdx)), current, nextTopology);
        }
        return nextTopology;
    }

    private boolean everyShardHasOverlaps(long prevEpoch, Shard[] in, Shard[] out)
    {
        int i = 0, o = 0;
        while (i < in.length && o < out.length)
        {
            Shard iv = in[i];
            Shard ov = out[o];
            // TODO (correctness) : this didn't work work removing ranges
            // Invariants.checkState(iv.range.compareIntersecting(ov.range) == 0);
            if (ov.nodes.stream().filter(iv::contains).allMatch(id -> topologyUpdates.isPending(ov.range, id)))
                return false;
            int c = iv.range.end().compareTo(ov.range.end());
            if (c <= 0) ++i;
            if (c >= 0) ++o;
        }
        // TODO (correctness) : think about this, i just got this complining
        return in.length != out.length;
//        Invariants.checkState (i == in.length && o == out.length);
//        return true;
    }
}
