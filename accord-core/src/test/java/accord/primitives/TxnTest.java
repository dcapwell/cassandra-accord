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

package accord.primitives;

import org.junit.jupiter.api.Test;

import accord.api.Data;
import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.impl.IntKey;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

class TxnTest
{
    @Test
    void blindWritesDontTriggerReads()
    {
        int epoch = 1;

        Keys keys = Keys.of(IntKey.key(42));
        Read read = Mockito.mock(Read.class);
        Mockito.when(read.keys()).thenReturn((Seekables) Keys.EMPTY);
        Mockito.when(read.read(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(args -> {throw new AssertionError("Attempted to read key " + args.getArgument(0));});
        Query query = Mockito.mock(Query.class);
        Update update = Mockito.mock(Update.class);
        Mockito.when(update.keys()).thenReturn((Seekables) keys);
        Txn txn = new Txn.InMemory(keys, read, query, update);

        SafeCommandStore store = Mockito.mock(SafeCommandStore.class);
        CommandStores.RangesForEpoch ranges = new CommandStores.RangesForEpoch(epoch, Ranges.single(IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE)), null);
        Mockito.when(store.ranges()).thenReturn(ranges);
        TxnId id = new TxnId(epoch, 1, Txn.Kind.Write, Routable.Domain.Key, new Node.Id(1));
        AsyncResult<Data> async = txn.read(store, id, Ranges.EMPTY).beginAsResult();
        Assertions.assertThat(async.isSuccess());
        // null means no reads.  If Read.read is called an exception should be thrown
        Assertions.assertThat(AsyncChains.getUnchecked(async)).isNull();
    }

}