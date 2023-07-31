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
import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Result;
import accord.api.TopologySorter;
import accord.coordinate.FetchData;
import accord.impl.IntKey;
import accord.impl.NoopProgressLog;
import accord.impl.basic.KeyType;
import accord.impl.basic.MockableCluster;
import accord.impl.basic.NodeBuilder;
import accord.impl.basic.NodeIdSorter;
import accord.impl.basic.SimpleSinks;
import accord.impl.list.ListAgent;
import accord.impl.list.ListData;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListStore;
import accord.impl.list.ListUpdate;
import accord.local.Command;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.ExtendedAssertions;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class FetchDataTest
{
    private static final Node.Id N1 = new Node.Id(1);
    private static final Node.Id N2 = new Node.Id(2);

    @Test
    void writeOnlyTxnFastPath() throws ExecutionException
    {
        Topology topology = new Topology(1,
                                         new Shard(IntKey.range(0, 100), Collections.singletonList(N1)),
                                         new Shard(IntKey.range(100, 200), Arrays.asList(N2, N1)));
        try (MockableCluster cluster = new MockableCluster.Builder(N1, N2)
                .withKeyType(KeyType.INT)
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withTopologySorter(NodeIdSorter.SUPPLIER)
                .buildAndStart())
        {
            Node n1 = cluster.node(N1);
            Node n2 = cluster.node(N2);

            TxnId txnId = n1.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
            IntKey.Raw shardOneKey = IntKey.key(42);
            IntKey.Raw shardTwoKey = IntKey.key(150);
            Keys writeKeys = Keys.of(shardOneKey, shardTwoKey);
            Keys readKeys = Keys.EMPTY.with(writeKeys);
            Txn txn = txn(readKeys, writeKeys);
            FullRoute<?> route = n1.computeRoute(txnId, txn.keys());
            Topologies topologies = n1.topology().withUnsyncedEpochs(route, txnId.epoch(), txnId.epoch());

            Node.Id replyTo = new Node.Id(-42);

            // TODO deps is 'unknown' as they were not committed!

            ExtendedAssertions.process(new PreAccept(N1, topologies, txnId, txn, route), n1, replyTo, PreAccept.PreAcceptReply.class)
                              .asInstanceOf(new InstanceOfAssertFactory<>(PreAccept.PreAcceptOk.class, Assertions::assertThat));

            ExtendedAssertions.process(new PreAccept(N2, topologies, txnId, txn, route), n2, replyTo, PreAccept.PreAcceptReply.class)
                              .asInstanceOf(new InstanceOfAssertFactory<>(PreAccept.PreAcceptOk.class, Assertions::assertThat));

            cluster.checkFailures();

            ListData data = emptyData(readKeys);

            Result result = txn.result(txnId, txnId, data);
            Writes writes = txn.execute(txnId, txnId, data);
            Deps deps = Deps.NONE;
            ExtendedAssertions.process(Apply.applyMaximal(N2, topologies, topologies, txnId, route, txn, txnId, deps, writes, result), n2, replyTo, Apply.ApplyReply.class)
                              .isEqualTo(Apply.ApplyReply.Applied);

            cluster.checkFailures();

            // TODO (now): slow path
            Arrays.asList(n1, n2).forEach(FetchDataTest::allowMessages);
            // the first time pushes the state from PreAccepted -> PreCommittedWithDefinition (if n2 is seen first)
            // the following times no-op
            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                    cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
                else cluster.sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

                Assertions.assertThat(fetch(n1, txnId, route, shardOneKey))
                          .extracting(k -> k.definition,
                                      k -> k.executeAt,
                                      k -> k.deps,
                                      k -> k.outcome)
                          .containsExactly(Status.Definition.DefinitionKnown,
                                           Status.KnownExecuteAt.ExecuteAtKnown,
                                           Status.KnownDeps.DepsUnknown,
                                           i % 2 == 0 ? Status.Outcome.Apply : Status.Outcome.Unknown);

                Assertions.assertThat(currentStatus(n1, txnId, shardOneKey))
                          .isEqualTo(SaveStatus.PreCommittedWithDefinition);

                cluster.checkFailures();
            }

            ExtendedAssertions.process(Apply.applyMaximal(N1, topologies, topologies, txnId, route, txn, txnId, deps, writes, result), n1, replyTo, Apply.ApplyReply.class)
                              .isEqualTo(Apply.ApplyReply.Applied);

            cluster.checkFailures();

            Assertions.assertThat(currentStatus(n1, txnId, shardOneKey)).isEqualTo(SaveStatus.Applied);
            Assertions.assertThat(currentStatus(n1, txnId, shardTwoKey)).isEqualTo(SaveStatus.Applied);
            Assertions.assertThat(currentStatus(n2, txnId, shardTwoKey)).isEqualTo(SaveStatus.Applied);

            Assertions.assertThat(cluster.stores.get(N1).data())
                      .isEqualTo(ImmutableMap.of(shardOneKey, new Timestamped<>(txnId, new int[]{1}),
                                                 shardTwoKey, new Timestamped<>(txnId, new int[]{1})));
            Assertions.assertThat(cluster.stores.get(N2).data())
                      .isEqualTo(ImmutableMap.of(shardTwoKey, new Timestamped<>(txnId, new int[]{1})));
        }
    }

    @Test
    void readWriteTxnFastPath() throws ExecutionException
    {
        Topology topology = new Topology(1,
                                         new Shard(IntKey.range(0, 100), Collections.singletonList(N1)),
                                         new Shard(IntKey.range(100, 200), Arrays.asList(N2, N1)));

        SimpleSinks sinks = new SimpleSinks();
        sinks.outboundFilter(r -> r instanceof TxnRequest); // ignore topology messages; easier while in a debugger
        Map<Node.Id, ListStore> stores = new HashMap<>();
        stores.put(N1, new ListStore(N1));
        stores.put(N2, new ListStore(N2));
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        ListAgent agent = new ListAgent(0, failures::add, ignore -> {});
        Node n1 = new NodeBuilder(N1)
                .withShardDistributorFromSplitter(ignore -> new IntKey.Splitter())
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withSink(sinks.mockedSinkFor(N1))
                .withTopologySorter(NodeIdSorter.SUPPLIER)
                .withDataSupplier(() -> stores.get(N1))
                .withAgent(agent)
                .buildAndStart();
        Node n2 = new NodeBuilder(N2)
                .withShardDistributorFromSplitter(ignore -> new IntKey.Splitter())
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withSink(sinks.mockedSinkFor(N2))
                .withTopologySorter(NodeIdSorter.SUPPLIER)
                .withDataSupplier(() -> stores.get(N2))
                .withAgent(agent)
                .buildAndStart();
        sinks.register(n1, n2);

        TxnId txnId = n1.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        IntKey.Raw shardOneReadKey = IntKey.key(40);
        IntKey.Raw shardOneWriteKey = IntKey.key(42);
        IntKey.Raw shardTwoReadKey = IntKey.key(120);
        IntKey.Raw shardTwoWriteKey = IntKey.key(150);
        Keys writeKeys = Keys.of(shardOneWriteKey, shardTwoWriteKey);
        Keys readKeys = Keys.of(shardOneReadKey, shardTwoReadKey).with(writeKeys);
        Txn txn = txn(readKeys, writeKeys);
        FullRoute<?> route = n1.computeRoute(txnId, txn.keys());
        Topologies topologies = n1.topology().withUnsyncedEpochs(route, txnId.epoch(), txnId.epoch());

        Node.Id replyTo = new Node.Id(-42);

        // TODO deps is 'unknown' as they were not committed!

        ExtendedAssertions.process(new PreAccept(N1, topologies, txnId, txn, route), n1, replyTo, PreAccept.PreAcceptReply.class)
                          .asInstanceOf(new InstanceOfAssertFactory<>(PreAccept.PreAcceptOk.class, Assertions::assertThat));

        ExtendedAssertions.process(new PreAccept(N2, topologies, txnId, txn, route), n2, replyTo, PreAccept.PreAcceptReply.class)
                          .asInstanceOf(new InstanceOfAssertFactory<>(PreAccept.PreAcceptOk.class, Assertions::assertThat));

        Assertions.assertThat(failures).isEmpty();

        // Commit.commitMinimalAndRead
        Data n2Data = process(new Commit(Commit.Kind.Minimal, N2, topology, topologies, txnId, txn, route, txn.keys()
                                                                                                              .toParticipants(), txnId, Deps.NONE, true), n2, replyTo, ReadData.ReadOk.class).data;

        Arrays.asList(n1, n2).forEach(FetchDataTest::allowMessages);
        // the first time pushes the state from PreAccepted -> PreCommittedWithDefinition (if n2 is seen first)
        // the following times no-op
        for (int i = 0; i < 10; i++)
        {
            if (i % 2 == 0)
                sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
            else sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

            Assertions.assertThat(fetch(n1, txnId, route, shardOneWriteKey))
                      .extracting(k -> k.definition,
                                  k -> k.executeAt,
                                  k -> k.deps,
                                  k -> k.outcome)
                      .containsExactly(Status.Definition.DefinitionKnown,
                                       Status.KnownExecuteAt.ExecuteAtKnown,
                                       Status.KnownDeps.DepsUnknown,
                                       Status.Outcome.Unknown);

            Assertions.assertThat(currentStatus(n1, txnId, shardOneWriteKey))
                      .isEqualTo(SaveStatus.PreCommittedWithDefinition);

            Assertions.assertThat(failures).isEmpty();
        }

        Data n1Data = process(new Commit(Commit.Kind.Minimal, N1, topology, topologies, txnId, txn, route, txn.keys()
                                                                                                              .toParticipants(), txnId, Deps.NONE, true), n1, replyTo, ReadData.ReadOk.class).data;
        Data data = n1Data.merge(n2Data);

        Assertions.assertThat(failures).isEmpty();

        Result result = txn.result(txnId, txnId, data);
        Writes writes = txn.execute(txnId, txnId, data);
        ExtendedAssertions.process(Apply.applyMaximal(N2, topologies, topologies, txnId, route, txn, txnId, Deps.NONE, writes, result), n2, replyTo, Apply.ApplyReply.class)
                          .isEqualTo(Apply.ApplyReply.Applied);

        Assertions.assertThat(failures).isEmpty();

        // TODO (now): slow path
        for (int i = 0; i < 10; i++)
        {
            if (i % 2 == 0)
                sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
            else sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

            Assertions.assertThat(fetch(n1, txnId, route, shardOneWriteKey))
                      .extracting(k -> k.definition,
                                  k -> k.executeAt,
                                  k -> k.deps,
                                  k -> k.outcome)
                      .containsExactly(Status.Definition.DefinitionKnown,
                                       Status.KnownExecuteAt.ExecuteAtKnown,
                                       Status.KnownDeps.DepsKnown,
                                       Status.Outcome.Apply);

            Assertions.assertThat(currentStatus(n1, txnId, shardOneWriteKey)).isEqualTo(SaveStatus.Applied);

            Assertions.assertThat(failures).isEmpty();
        }


        ExtendedAssertions.process(Apply.applyMaximal(N1, topologies, topologies, txnId, route, txn, txnId, Deps.NONE, writes, result), n1, replyTo, Apply.ApplyReply.class)
                          .isEqualTo(Apply.ApplyReply.Redundant);

        Assertions.assertThat(failures).isEmpty();

        Assertions.assertThat(currentStatus(n1, txnId, shardOneWriteKey)).isEqualTo(SaveStatus.Applied);
        Assertions.assertThat(currentStatus(n1, txnId, shardTwoWriteKey)).isEqualTo(SaveStatus.Applied);
        Assertions.assertThat(currentStatus(n2, txnId, shardTwoWriteKey)).isEqualTo(SaveStatus.Applied);

        Assertions.assertThat(stores.get(N1).data())
                  .isEqualTo(ImmutableMap.of(shardOneWriteKey, new Timestamped<>(txnId, new int[]{1}),
                                             shardTwoWriteKey, new Timestamped<>(txnId, new int[]{1})));
        Assertions.assertThat(stores.get(N2).data())
                  .isEqualTo(ImmutableMap.of(shardTwoWriteKey, new Timestamped<>(txnId, new int[]{1})));
    }

    private static ListData emptyData(Keys keys)
    {
        ListData data = new ListData();
        for (Key key : keys)
            data.put(key, ListStore.EMPTY);
        return data;
    }

    private Txn txn(Keys reads, Keys writes)
    {
        Keys keys = reads.with(writes);
        ListRead read = new ListRead(Function.identity(), reads, keys);
        ListUpdate update = writes.isEmpty() ? null : new ListUpdate(Function.identity());
        if (update != null)
        {
            for (Key k : writes)
                update.put(k, 1);
        }
        ListQuery query = new ListQuery(new Node.Id(-1), 0);
        return new Txn.InMemory(keys, read, query, update);
    }

    private static Status.Known fetch(Node node, TxnId txnId, FullRoute<?> route, Key key) throws ExecutionException
    {
        AsyncResult.Settable<Status.Known> fetched = AsyncResults.settable();
        node.commandStores().unsafeForKey(key).execute(() -> {
            try
            {
                FetchData.fetch(SaveStatus.PreAccepted.known, node, txnId, route, fetched.settingCallback());
            }
            catch (Throwable t)
            {
                fetched.tryFailure(t);
            }
        });
        return AsyncChains.getUninterruptibly(fetched);
    }

    private static SaveStatus currentStatus(Node node, TxnId txnId, Key key) throws ExecutionException
    {
        AsyncResult.Settable<SaveStatus> minStatus = AsyncResults.settable();
        node.commandStores().unsafeForKey(key).submit(PreLoadContext.contextFor(txnId), safe -> {
            Command command = safe.get(txnId, key.toUnseekable()).current();
            return command == null ? null : command.saveStatus();
        }).begin(minStatus.settingCallback());
        return AsyncChains.getUninterruptibly(minStatus);
    }

    private static void allowMessages(Node node)
    {
        MessageSink sink = node.messageSink();
        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS).when(sink).send(Mockito.any(), Mockito.any());
        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS)
               .when(sink)
               .send(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS).when(sink).reply(Mockito.any(), Mockito.any(), Mockito.any());
    }

    public static <T extends Reply> T process(TxnRequest<?> request, Node on, Node.Id replyTo, Class<T> replyType)
    {
        ReplyContext replyContext = Mockito.mock(ReplyContext.class);
        request.process(on, replyTo, replyContext);
        ArgumentCaptor<T> reply = ArgumentCaptor.forClass(replyType);
        Mockito.verify(on.messageSink()).reply(Mockito.eq(replyTo), Mockito.eq(replyContext), reply.capture());
        return reply.getValue();
    }
}
