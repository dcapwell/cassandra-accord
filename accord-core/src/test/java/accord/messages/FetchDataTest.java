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

package accord.messages;

import accord.api.Data;
import accord.api.Result;
import accord.impl.IntKey;
import accord.impl.NoopProgressLog;
import accord.impl.basic.KeyType;
import accord.impl.basic.MockableCluster;
import accord.impl.basic.NodeIdSorter;
import accord.impl.basic.SimpleSinks;
import accord.impl.list.ListData;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Timestamped;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static accord.local.Commands.AcceptOutcome.Success;
import static accord.utils.ExtendedAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class FetchDataTest
{
    // TODO (now): slow path
    private static final Node.Id N1 = new Node.Id(1);
    private static final Node.Id N2 = new Node.Id(2);

    @Test
    void writeOnlyTxnFastPath() throws ExecutionException, InterruptedException
    {
        // PreAccept -> Apply
        writeOnlyTxn(true);
    }

    @Test
    void writeOnlyTxnSlowPath() throws ExecutionException, InterruptedException
    {
        // PreAccept -> Accept -> Apply
        writeOnlyTxn(false);
    }

    private void writeOnlyTxn(boolean fastPath) throws ExecutionException, InterruptedException
    {
        Topology topology = new Topology(1,
                                         new Shard(IntKey.range(0, 100), Collections.singletonList(N1)),
                                         new Shard(IntKey.range(100, 200), Arrays.asList(N2, N1)));
        try (MockableCluster cluster = new MockableCluster.Builder(N1, N2)
                .withKeyType(KeyType.INT)
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withTopologySorter(NodeIdSorter.SUPPLIER)
                .withSinkMockType(SimpleSinks.MockType.NO_OP)
                .buildAndStart())
        {
            Node n1 = cluster.node(N1);
            Node n2 = cluster.node(N2);

            TxnId txnId = n1.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            IntKey.Raw shardOneKey = IntKey.key(42);
            IntKey.Raw shardTwoKey = IntKey.key(150);
            Keys writeKeys = Keys.of(shardOneKey, shardTwoKey);
            Keys readKeys = Keys.EMPTY.with(writeKeys);
            Txn txn = cluster.txn(readKeys, writeKeys);
            FullRoute<?> route = n1.computeRoute(txnId, txn.keys());
            Topologies topologies = n1.topology().withUnsyncedEpochs(route, txnId.epoch(), txnId.epoch());

            Arrays.asList(n1, n2)
                  .forEach(node -> assertThat(cluster.process(node, PreAccept.PreAcceptReply.class, id -> new PreAccept(id, topologies, txnId, txn, route))).isOk());

            cluster.checkFailures();

            if (!fastPath)
            {
                Ballot ballot = Ballot.ZERO; // TODO figure out
                Arrays.asList(n1, n2)
                      .forEach(node -> {
                          Ranges ranges = Ranges.EMPTY;
                          Ranges localRanges = topologies.computeRangesForNode(node.id()).intersecting(txn.keys());
                          for (Seekable s : txn.keys())
                          {
                              if (!localRanges.contains(s.asKey().toUnseekable())) continue;
                              ranges = ranges.with(node.commandStores().unsafeForKey(s.asKey()).unsafeRangesForEpoch().currentRanges());
                          }
                          assertThat(cluster.process(node, Accept.AcceptReply.class, id -> new Accept(id, topologies, ballot, txnId, route, txnId, txn.keys(), Deps.NONE)))
                                  .extracting(r -> r.outcome, r -> r.supersededBy, r -> r.deps)
                                  .containsExactly(Success, null, new PartialDeps(ranges, KeyDeps.NONE, RangeDeps.NONE));
                      });
                cluster.checkFailures();
            }

            ListData data = cluster.emptyData(readKeys);

            Result result = txn.result(txnId, txnId, data);
            Writes writes = txn.execute(txnId, txnId, data);
            assertThat(cluster.process(n2, Apply.ApplyReply.class, id -> Apply.applyMaximal(id, topologies, topologies, txnId, route, txn, txnId, Deps.NONE, writes, result)))
                    .isEqualTo(Apply.ApplyReply.Applied);

            cluster.checkFailures();
            cluster.allowMessages();

            // the first time pushes the state from PreAccepted -> PreCommittedWithDefinition (if n2 is seen first)
            // the following times no-op
            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                    cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
                else cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

                assertThat(cluster.fetchBlocking(N1, txnId, route, shardOneKey))
                        .extracting(k -> k.definition,
                                    k -> k.executeAt,
                                    k -> k.deps,
                                    k -> k.outcome)
                        .containsExactly(Status.Definition.DefinitionKnown,
                                         Status.KnownExecuteAt.ExecuteAtKnown,
                                         Status.KnownDeps.DepsUnknown,
                                         i % 2 == 0 ? Status.Outcome.Apply : Status.Outcome.Unknown);

                assertThat(cluster.currentStatusBlocking(N1, txnId, shardOneKey))
                        .isEqualTo(fastPath ? SaveStatus.PreCommittedWithDefinition : SaveStatus.PreCommittedWithDefinitionAndAcceptedDeps);

                cluster.checkFailures();
            }

            assertThat(cluster.process(n1, Apply.ApplyReply.class, id -> Apply.applyMaximal(id, topologies, topologies, txnId, route, txn, txnId, Deps.NONE, writes, result)))
                    .isEqualTo(Apply.ApplyReply.Applied);

            cluster.checkFailures();

            assertThat(cluster.currentStatusBlocking(N1, txnId, shardOneKey)).isEqualTo(SaveStatus.Applied);
            assertThat(cluster.currentStatusBlocking(N1, txnId, shardTwoKey)).isEqualTo(SaveStatus.Applied);
            assertThat(cluster.currentStatusBlocking(N2, txnId, shardTwoKey)).isEqualTo(SaveStatus.Applied);

            assertThat(cluster.stores.get(N1).data())
                    .isEqualTo(ImmutableMap.of(shardOneKey, new Timestamped<>(txnId, new int[]{1}),
                                               shardTwoKey, new Timestamped<>(txnId, new int[]{1})));
            assertThat(cluster.stores.get(N2).data())
                    .isEqualTo(ImmutableMap.of(shardTwoKey, new Timestamped<>(txnId, new int[]{1})));
        }
    }

    @Test
    void readWriteTxnFastPath() throws ExecutionException, InterruptedException
    {
        // PreAccept -> Commit -> Apply
        readWriteTxn(true);
    }

    @Test
    void readWriteTxnSlowPath() throws ExecutionException, InterruptedException
    {
        // PreAccept -> Apply -> Commit -> Apply
        readWriteTxn(false);
    }

    private void readWriteTxn(boolean fastPath) throws ExecutionException, InterruptedException
    {
        Topology topology = new Topology(1,
                                         new Shard(IntKey.range(0, 100), Collections.singletonList(N1)),
                                         new Shard(IntKey.range(100, 200), Arrays.asList(N2, N1)));
        try (MockableCluster cluster = new MockableCluster.Builder(N1, N2)
                .withKeyType(KeyType.INT)
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withTopologySorter(NodeIdSorter.SUPPLIER)
                .withSinkMockType(SimpleSinks.MockType.NO_OP)
                .buildAndStart())
        {
            Node n1 = cluster.node(N1);
            Node n2 = cluster.node(N2);

            TxnId txnId = n1.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            IntKey.Raw shardOneReadKey = IntKey.key(40);
            IntKey.Raw shardOneWriteKey = IntKey.key(42);
            IntKey.Raw shardTwoReadKey = IntKey.key(120);
            IntKey.Raw shardTwoWriteKey = IntKey.key(150);
            Keys writeKeys = Keys.of(shardOneWriteKey, shardTwoWriteKey);
            Keys readKeys = Keys.of(shardOneReadKey, shardTwoReadKey).with(writeKeys);
            Txn txn = cluster.txn(readKeys, writeKeys);
            FullRoute<?> route = n1.computeRoute(txnId, txn.keys());
            Topologies topologies = n1.topology().withUnsyncedEpochs(route, txnId.epoch(), txnId.epoch());

            Arrays.asList(n1, n2)
                  .forEach(node -> assertThat(cluster.process(node, PreAccept.PreAcceptReply.class, id -> new PreAccept(id, topologies, txnId, txn, route))).isOk());

            cluster.checkFailures();

            if (!fastPath)
            {
                Ballot ballot = Ballot.ZERO; // TODO figure out
                Arrays.asList(n1, n2)
                      .forEach(node -> {
                          Ranges ranges = Ranges.EMPTY;
                          Ranges localRanges = topologies.computeRangesForNode(node.id()).intersecting(txn.keys());
                          for (Seekable s : txn.keys())
                          {
                              if (!localRanges.contains(s.asKey().toUnseekable())) continue;
                              ranges = ranges.with(node.commandStores().unsafeForKey(s.asKey()).unsafeRangesForEpoch().currentRanges());
                          }
                          assertThat(cluster.process(node, Accept.AcceptReply.class, id -> new Accept(id, topologies, ballot, txnId, route, txnId, txn.keys(), Deps.NONE)))
                                  .extracting(r -> r.outcome, r -> r.supersededBy, r -> r.deps)
                                  .containsExactly(Success, null, new PartialDeps(ranges, KeyDeps.NONE, RangeDeps.NONE));
                      });
                cluster.checkFailures();
            }

            // Commit.commitMinimalAndRead
            Data n2Data = cluster.process(n2, ReadData.ReadOk.class, id -> new Commit(Commit.Kind.Minimal, id, topology, topologies, txnId, txn, route, txn.keys().toParticipants(), txnId, Deps.NONE, true)).data;

            cluster.allowMessages();
            // the first time pushes the state from PreAccepted -> PreCommittedWithDefinition (if n2 is seen first)
            // the following times no-op
            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                    cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
                else cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

                assertThat(cluster.fetchBlocking(N1, txnId, route, shardOneWriteKey))
                          .extracting(k -> k.definition,
                                      k -> k.executeAt,
                                      k -> k.deps,
                                      k -> k.outcome)
                          .containsExactly(Status.Definition.DefinitionKnown,
                                           Status.KnownExecuteAt.ExecuteAtKnown,
                                           Status.KnownDeps.DepsUnknown,
                                           Status.Outcome.Unknown);

                assertThat(cluster.currentStatusBlocking(N1, txnId, shardOneWriteKey))
                          .isEqualTo(fastPath ? SaveStatus.PreCommittedWithDefinition: SaveStatus.PreCommittedWithDefinitionAndAcceptedDeps);

                cluster.checkFailures();
            }

            Data n1Data = cluster.process(n1, ReadData.ReadOk.class, id -> new Commit(Commit.Kind.Minimal, id, topology, topologies, txnId, txn, route, txn.keys().toParticipants(), txnId, Deps.NONE, true)).data;
            Data data = n1Data.merge(n2Data);

            cluster.checkFailures();

            Result result = txn.result(txnId, txnId, data);
            Writes writes = txn.execute(txnId, txnId, data);
            assertThat(cluster.process(n2, Apply.ApplyReply.class, id -> Apply.applyMaximal(id, topologies, topologies, txnId, route, txn, txnId, Deps.NONE, writes, result)))
                      .isEqualTo(Apply.ApplyReply.Applied);

            cluster.checkFailures();

            for (int i = 0; i < 10; i++)
            {
                if (i == 1 && !fastPath)
                {
                    // not possible in fast-path as we need to be committed to know deps
//                    cluster.markRangeDurableBlocking(N2, n2.topology().currentLocal().ranges());
                    cluster.markRangeDurableBlocking(N2, Ranges.of(IntKey.range(100, 122)));

                    cluster.checkFailures();
                }
                if (i % 2 == 0)
                    cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
                else cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

                assertThat(cluster.fetchBlocking(N1, txnId, route, shardOneWriteKey))
                          .extracting(k -> k.definition,
                                      k -> k.executeAt,
                                      k -> k.deps,
                                      k -> k.outcome)
                          .containsExactly(Status.Definition.DefinitionKnown,
                                           Status.KnownExecuteAt.ExecuteAtKnown,
                                           Status.KnownDeps.DepsKnown,
                                           Status.Outcome.Apply);

                assertThat(cluster.currentStatusBlocking(N1, txnId, shardOneWriteKey)).isEqualTo(SaveStatus.Applied);

                cluster.checkFailures();
            }


            assertThat(cluster.process(n1, Apply.ApplyReply.class, id -> Apply.applyMaximal(id, topologies, topologies, txnId, route, txn, txnId, Deps.NONE, writes, result)))
                      .isEqualTo(Apply.ApplyReply.Redundant);

            cluster.checkFailures();

            assertThat(cluster.currentStatusBlocking(N1, txnId, shardOneWriteKey)).isEqualTo(SaveStatus.Applied);
            assertThat(cluster.currentStatusBlocking(N1, txnId, shardTwoWriteKey)).isEqualTo(SaveStatus.Applied);
            assertThat(cluster.currentStatusBlocking(N2, txnId, shardTwoWriteKey)).isEqualTo(SaveStatus.Applied);

            assertThat(cluster.stores.get(N1).data())
                      .isEqualTo(ImmutableMap.of(shardOneWriteKey, new Timestamped<>(txnId, new int[]{1}),
                                                 shardTwoWriteKey, new Timestamped<>(txnId, new int[]{1})));
            assertThat(cluster.stores.get(N2).data())
                      .isEqualTo(ImmutableMap.of(shardTwoWriteKey, new Timestamped<>(txnId, new int[]{1})));
        }
    }

}
