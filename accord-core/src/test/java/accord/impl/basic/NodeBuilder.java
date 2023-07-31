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

import accord.api.ConfigurationService;
import accord.api.TestableConfigurationService;
import accord.impl.mock.MockConfigurationService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.EpochFunction;

import static accord.utils.async.AsyncChains.awaitUninterruptibly;

public class NodeBuilder extends AbstractBuilder<NodeBuilder>
{
    final Node.Id id;

    public NodeBuilder(Node.Id id)
    {
        this.id = id;
    }

    public NodeBuilder(AbstractBuilder<?> other, Node.Id id)
    {
        super(other);
        this.id = id;
    }

    public Node build()
    {
        ConfigurationService configService = this.configService;
        if (configService == null)
            configService = new MockConfigurationService(messageSink, EpochFunction.noop());
        return new Node(id, messageSink, configService, nowSupplier, dataSupplier, shardDistributor, agent, random, scheduler, topologySorter, progressLogFactory, factory);
    }

    public Node buildAndStart()
    {
        Node node = build();
        awaitUninterruptibly(node.start());
        for (Topology t : topologies)
            ((TestableConfigurationService) node.configService()).reportTopology(t);
        return node;
    }
}
