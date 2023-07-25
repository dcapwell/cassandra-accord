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

package accord.local;

import accord.Utils;
import accord.api.Key;
import accord.api.TestableConfigurationService;
import accord.impl.PrefixedIntHashKey;
import accord.topology.TopologyUtils;
import accord.messages.PreAccept;
import accord.messages.ReplyContext;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Comparator;
import java.util.List;

import static accord.Utils.writeTxn;
import static accord.utils.Property.qt;
import static accord.utils.Utils.addAll;
import static org.assertj.core.api.Assertions.assertThat;

class CommandsTest
{
    private static final Node.Id N1 = new Node.Id(1);

    @Test
    void addAndRemoveRangesValidate()
    {
        Gen<List<Node.Id>> nodeGen = Gens.lists(AccordGens.nodes()).ofSizeBetween(1, 100);
        qt().check(rs -> {
            List<Node.Id> nodes = nodeGen.next(rs);
            if (!nodes.contains(N1))
                nodes.add(N1);
            nodes.sort(Comparator.naturalOrder());
            int rf = Math.min(3, nodes.size());
            Range[] ranges = PrefixedIntHashKey.ranges(0, nodes.size());
            Range[] allRanges = addAll(ranges, PrefixedIntHashKey.ranges(1, nodes.size()));
            boolean add = rs.nextBoolean();
            Topology initialTopology = TopologyUtils.topology(1, nodes, Ranges.of(add ? ranges : allRanges), rf);
            Topology updatedTopology = TopologyUtils.topology(2, nodes, Ranges.of(add ? allRanges : ranges), rf);

            Node node = new Utils.NodeBuilder(N1)
                    .withShardDistributorFromSplitter(ignore -> new PrefixedIntHashKey.Splitter())
                    .buildAndStart();

            ((TestableConfigurationService) node.configService()).reportTopology(initialTopology);

            Ranges localRange = node.topology().localRangesForEpoch(initialTopology.epoch());
            Gen<Key> keyGen = AccordGens.prefixedIntHashKey(ignore -> 0).filter(localRange::contains);
            Keys keys = Keys.of(Gens.lists(keyGen).unique().ofSizeBetween(1, 10).next(rs));
            Txn txn = writeTxn(keys);

            TxnId txnId = node.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            FullRoute<?> route = node.computeRoute(txnId, txn.keys());
            ((TestableConfigurationService) node.configService()).reportTopology(updatedTopology);

            PreAccept preAccept = new PreAccept(N1, node.topology().withUnsyncedEpochs(route, txnId.epoch(), updatedTopology.epoch()), txnId, txn, route);
            ReplyContext replyContext = Mockito.mock(ReplyContext.class);
            preAccept.process(node, N1, replyContext);

            ArgumentCaptor<PreAccept.PreAcceptOk> reply = ArgumentCaptor.forClass(PreAccept.PreAcceptOk.class);
            Mockito.verify(node.messageSink()).reply(Mockito.eq(N1), Mockito.eq(replyContext), reply.capture());

            PreAccept.PreAcceptOk result = reply.getValue();
            assertThat(result.txnId).isEqualTo(txnId);
            assertThat(result.witnessedAt.epoch()).isEqualTo(updatedTopology.epoch());
            assertThat(result.deps.isEmpty()).isTrue();
        });
    }
}