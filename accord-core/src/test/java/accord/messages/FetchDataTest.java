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
import accord.api.MessageSink;
import accord.api.Result;
import accord.coordinate.FetchData;
import accord.impl.IntKey;
import accord.impl.NoopProgressLog;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static accord.Utils.writeTxn;

public class FetchDataTest
{
    private static final Node.Id N1 = new Node.Id(1);
    private static final Node.Id N2 = new Node.Id(2);

    @Test
    void oneShardAcceptedOtherPreAcceptFastPath() throws ExecutionException
    {
//        Topology topology = TopologyUtils.topology(1, Arrays.asList(N1, N2), Ranges.of(ranges), 1);
        Topology topology = new Topology(1,
                                         new Shard(IntKey.range(0, 100), Collections.singletonList(N1)),
                                         new Shard(IntKey.range(100, 200), Arrays.asList(N1, N2)));
        Txn txn = writeTxn(Keys.of(IntKey.key(42), IntKey.key(150)));

        Node n1 = new Utils.NodeBuilder(N1)
                .withShardDistributorFromSplitter(ignore -> new IntKey.Splitter())
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .buildAndStart();
        Node n2 = new Utils.NodeBuilder(N2)
                .withShardDistributorFromSplitter(ignore -> new IntKey.Splitter())
                .withProgressLog(NoopProgressLog.INSTANCE)
                .withTopologies(topology)
                .buildAndStart();

        TxnId txnId = n1.nextTxnId(Txn.Kind.Write, Routable.Domain.Key);
        FullRoute<?> route = n1.computeRoute(txnId, txn.keys());
        Topologies topologies = n1.topology().withUnsyncedEpochs(route, txnId.epoch(), txnId.epoch());

        Node.Id replyTo = new Node.Id(-42);

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
        registerSinks(n1, n2);
        // TODO (now): recoverWithRoute
        AsyncResult.Settable<Status.Known> fetched = AsyncResults.settable();
        n1.commandStores().unsafeForKey(IntKey.key(42)).execute(() -> {
            try
            {
                FetchData.fetch(SaveStatus.PreAccepted.known, n1, txnId, route, fetched.settingCallback());
            }
            catch (Throwable t)
            {
                fetched.tryFailure(t);
            }
        });
        Status.Known known = AsyncChains.getUninterruptibly(fetched);
        System.out.println();
    }

    private static class MsgState
    {
        final Node node;
        AtomicInteger msgIds = new AtomicInteger();
        Map<Long, SafeCallback> callbacks = new ConcurrentHashMap<>();

        private MsgState(Node node)
        {
            this.node = node;
        }
    }

    private static class ExpectedReply implements ReplyContext
    {
        final Node to;
        final long id;

        private ExpectedReply(Node to, long id)
        {
            this.to = to;
            this.id = id;
        }
    }

    private static void registerSinks(Node... ns)
    {
        Map<Node.Id, Node> nodes = new HashMap<>();
        Map<Node, MsgState> sinks = new HashMap<>();
        for (Node n : ns)
        {
            nodes.put(n.id(), n);
            sinks.put(n, new MsgState(n));
        }
        for (Node n : ns)
            registerSinks(nodes, sinks, n);
    }

    private static void registerSinks(Map<Node.Id, Node> nodes, Map<Node, MsgState> sinks, Node on)
    {
        MsgState onState = sinks.computeIfAbsent(on, MsgState::new);
        MessageSink sink = on.messageSink();
        Mockito.doAnswer(args -> {
                   Node to = nodes.get(args.getArgument(0));
                   to.receive(args.getArgument(1), on.id(), Mockito.mock(ReplyContext.class));
                   return null;
               })
               .when(sink).send(Mockito.any(), Mockito.any());

        Mockito.doAnswer(args -> {
                   Node to = nodes.get(args.getArgument(0));
                   long msgId = onState.msgIds.incrementAndGet();
                   onState.callbacks.put(msgId, new SafeCallback(args.getArgument(2), args.getArgument(3)));
                   to.receive(args.getArgument(1), on.id(), new ExpectedReply(on, msgId));
                   return null;
               })
               .when(sink).send(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        Mockito.doAnswer(args -> {
                   ExpectedReply replyContext = args.getArgument(1);
                   MsgState state = sinks.get(replyContext.to);
                   Map<Long, SafeCallback> callbacks = state.callbacks;
                   Reply reply = args.getArgument(2);
                   SafeCallback callback = reply.isFinal() ? callbacks.remove(replyContext.id) : callbacks.get(replyContext.id);
                   if (callback != null)
                       callback.success(on.id(), reply);
                   return null;
               })
               .when(sink).reply(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
