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

import accord.impl.IntHashKey;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Node;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.topology.Topologies;
import accord.topology.Topology;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class TxnRequestTest
{
    @Test
    void emptyTxnFailsToCreateScope()
    {
        Node.Id id = new Node.Id(42);
        Topologies topologies = new Topologies.Multi(SizeOfIntersectionSorter.SUPPLIER,
                                                     new Topology(12),
                                                     new Topology(11),
                                                     new Topology(10));
        // txn has [] ranges, so we select a random home key, so the route becomes [] with home key=42
        FullRoute<?> route = Ranges.EMPTY.toRoute(IntHashKey.forHash(42));

        Assertions.assertThatThrownBy(() -> TxnRequest.computeScope(id, topologies, route))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage("No intersection");
    }
}