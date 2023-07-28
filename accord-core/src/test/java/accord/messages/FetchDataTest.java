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

import accord.Utils;
import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Result;
import accord.api.TopologySorter;
import accord.coordinate.FetchData;
import accord.impl.IntKey;
import accord.impl.NoopProgressLog;
import accord.impl.basic.SimpleSinks;
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
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static accord.Utils.writeTxn;

public class FetchDataTest
{
    private static final Node.Id N1 = new Node.Id(1);
    private static final Node.Id N2 = new Node.Id(2);

    @Test
    void oneShardAcceptedOtherPreAcceptFastPath() throws ExecutionException
    {
        Topology topology = new Topology(1,
                                         new Shard(IntKey.range(0, 100), Collections.singletonList(N1)),
                                         new Shard(IntKey.range(100, 200), Arrays.asList(N2, N1)));

        SimpleSinks sinks = new SimpleSinks();
        sinks.outboundFilter(r -> r instanceof TxnRequest); // ignore topology messages; easier while in a debugger
        TopologySorter byNodeId = (a, b, shards) -> a.compareTo(b);
        TopologySorter.Supplier byNodeIdSupplier = new TopologySorter.Supplier()
        {
            @Override
            public TopologySorter get(Topology topologies)
            {
                return byNodeId;
            }

            @Override
            public TopologySorter get(Topologies topologies)
            {
                return byNodeId;
            }
        };
        Node n1 = new Utils.NodeBuilder(N1)
                .withShardDistributorFromSplitter(ignore -> new IntKey.Splitter())
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withSink(sinks.mockedSinkFor(N1))
                .withTopologySorter(byNodeIdSupplier)
                .buildAndStart();
        Node n2 = new Utils.NodeBuilder(N2)
                .withShardDistributorFromSplitter(ignore -> new IntKey.Splitter())
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .withSink(sinks.mockedSinkFor(N2))
                .withTopologySorter(byNodeIdSupplier)
                .buildAndStart();
        sinks.register(n1, n2);

        TxnId txnId = n1.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        IntKey.Raw shardOneKey = IntKey.key(42);
        IntKey.Raw shardTwoKey = IntKey.key(150);
        Txn txn = writeTxn(Keys.of(shardOneKey, shardTwoKey));
        FullRoute<?> route = n1.computeRoute(txnId, txn.keys());
        Topologies topologies = n1.topology().withUnsyncedEpochs(route, txnId.epoch(), txnId.epoch());

        Node.Id replyTo = new Node.Id(-42);

        // TODO deps is 'unknown' as they were not committed!

        ExtendedAssertions.process(new PreAccept(N1, topologies, txnId, txn, route), n1, replyTo, PreAccept.PreAcceptReply.class)
                          .asInstanceOf(new InstanceOfAssertFactory<>(PreAccept.PreAcceptOk.class, Assertions::assertThat));

        ExtendedAssertions.process(new PreAccept(N2, topologies, txnId, txn, route), n2, replyTo, PreAccept.PreAcceptReply.class)
                          .asInstanceOf(new InstanceOfAssertFactory<>(PreAccept.PreAcceptOk.class, Assertions::assertThat));

        // TODO this is when read.isEmpty()
        Result result = txn.result(txnId, txnId, null);
        Writes writes = txn.execute(txnId, txnId, null);
        Deps deps = Deps.NONE;
        ExtendedAssertions.process(Apply.applyMaximal(N2, topologies, topologies, txnId, route, txn, txnId, deps, writes, result), n2, replyTo, Apply.ApplyReply.class)
                          .isEqualTo(Apply.ApplyReply.Applied);

        // TODO (now): slow path
        Arrays.asList(n1, n2).forEach(FetchDataTest::allowMessages);
        // the first time pushes the state from PreAccepted -> PreCommittedWithDefinition (if n2 is seen first)
        // the following times no-op
        for (int i = 0; i < 10; i++)
        {
            if (i % 2 == 0) sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N2, N1); // includes i=0, which is needed to make sure we push forward the first time
            else            sinks.replyOrdering(N1, CheckStatus.CheckStatusReply.class, N1, N2);

            Assertions.assertThat(fetch(n1, txnId, route, shardOneKey))
                      .extracting(k -> k.definition,
                                  k -> k.executeAt,
                                  k -> k.deps,
                                  k -> k.outcome)
                      .containsExactly(Status.Definition.DefinitionKnown,
                                       Status.KnownExecuteAt.ExecuteAtKnown,
                                       Status.KnownDeps.DepsUnknown,
                                       i % 2 == 0 ? Status.Outcome.Apply : Status.Outcome.Unknown);

            Assertions.assertThat(currentStatus(n1, txnId, shardOneKey)).isEqualTo(SaveStatus.PreCommittedWithDefinition);
        }

        ExtendedAssertions.process(Apply.applyMaximal(N1, topologies, topologies, txnId, route, txn, txnId, deps, writes, result), n1, replyTo, Apply.ApplyReply.class)
                          .isEqualTo(Apply.ApplyReply.Applied);

        Assertions.assertThat(currentStatus(n1, txnId, shardOneKey)).isEqualTo(SaveStatus.Applied);
        Assertions.assertThat(currentStatus(n1, txnId, shardTwoKey)).isEqualTo(SaveStatus.Applied);
        Assertions.assertThat(currentStatus(n2, txnId, shardTwoKey)).isEqualTo(SaveStatus.Applied);
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
        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS).when(sink).send(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.doAnswer(Mockito.CALLS_REAL_METHODS).when(sink).reply(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
