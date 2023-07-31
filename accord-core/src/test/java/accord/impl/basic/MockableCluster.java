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

import accord.api.TestableConfigurationService;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.local.Node;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.TxnRequest;
import accord.topology.Topology;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class MockableCluster implements AutoCloseable
{
    public final SimpleSinks sinks;
    public final List<Throwable> failures;
    public final ListAgent agent;
    public final Map<Node.Id, ListStore> stores;
    public final Map<Node.Id, Node> nodes;

    public MockableCluster(SimpleSinks sinks,
                           List<Throwable> failures,
                           ListAgent agent,
                           Map<Node.Id, ListStore> stores,
                           Map<Node.Id, Node> nodes)
    {
        this.sinks = sinks;
        this.failures = failures;
        this.agent = agent;
        this.stores = stores;
        this.nodes = nodes;
    }

    public Node node(Node.Id id)
    {
        return Objects.requireNonNull(nodes.get(id), "Unknown node: " + id);
    }

    public void checkFailures()
    {
        Assertions.assertThat(failures).isEmpty();
    }

    public <T extends Reply> T process(Node on, Node.Id replyTo, Class<T> replyType, Function<Node.Id, TxnRequest<?>> creator)
    {
        TxnRequest<?> request = creator.apply(on.id());
        ReplyContext replyContext = Mockito.mock(ReplyContext.class);
        // process is normally an async operation, but MockableCluster tends to use a blocking CommandStore, making this a sync operation
        // for this reason, the reply is expected after this method returns; if the author overrides this behavior, then this may become
        // async again and this method will fail as reply was not called.
        request.process(on, replyTo, replyContext);
        ArgumentCaptor<T> reply = ArgumentCaptor.forClass(replyType);
        Mockito.verify(on.messageSink()).reply(Mockito.eq(replyTo), Mockito.eq(replyContext), reply.capture());
        return reply.getValue();
    }

    @Override
    public void close()
    {
        for (Node n : nodes.values())
            n.shutdown();
    }

    public static class Builder extends AbstractBuilder<Builder>
    {
        final Set<Node.Id> nodeIds;

        public Builder(Node.Id first, Node.Id... rest)
        {
            ImmutableSet.Builder<Node.Id> builder = ImmutableSet.<Node.Id>builder().add(first);
            if (rest.length != 0)
                builder.addAll(Arrays.asList(rest));
            nodeIds = builder.build();
        }

        public MockableCluster build()
        {
            SimpleSinks sinks = new SimpleSinks();

            List<Throwable> failures = new CopyOnWriteArrayList<>();
            ListAgent agent = new ListAgent(0, failures::add, ignore -> {});

            Map<Node.Id, Node> nodes = Maps.newHashMapWithExpectedSize(nodeIds.size());
            Map<Node.Id, ListStore> stores = Maps.newHashMapWithExpectedSize(nodeIds.size());
            for (Node.Id id : nodeIds)
            {
                ListStore store = new ListStore(id);
                stores.put(id, store);

                Node node = new NodeBuilder(this, id)
                        .withSink(sinks.mockedSinkFor(id))
                        .withDataSupplier(() -> store)
                        .withAgent(agent)
                        .build();
                nodes.put(id, node);
                sinks.register(node);
            }
            return new MockableCluster(sinks, failures, agent, stores, nodes);
        }

        public MockableCluster buildAndStart()
        {
            MockableCluster cluster = build();
            List<AsyncChain<Void>> startups = new ArrayList<>(cluster.nodes.size());
            for (Node n : cluster.nodes.values())
                startups.add(n.start());
            try
            {
                AsyncChains.getUninterruptibly(AsyncChains.all(startups));
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e.getCause());
            }
            for (Topology t : topologies)
            {
                for (Node node : cluster.nodes.values())
                {
                    ((TestableConfigurationService) node.configService()).reportTopology(t);
                }
            }
            return cluster;
        }
    }
}
