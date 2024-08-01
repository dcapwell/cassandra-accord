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

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import accord.api.Key;
import accord.impl.IntKey;
import accord.impl.list.ListAgent;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListUpdate;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import org.mockito.Mockito;

public class TxnGenBuilder
{
    public static final Gen<Routable.Domain> DOMAIN_GEN = Gens.enums().all(Routable.Domain.class);
    public static final Gen<Gen<Routable.Domain>> DOMAIN_DISTRIBUTION = Gens.enums().allMixedDistribution(Routable.Domain.class);
    public static final List<Txn.Kind> ALLOWED_KEY_KINDS = ImmutableList.of(Txn.Kind.Write, Txn.Kind.Read, Txn.Kind.SyncPoint);
    public static final Gen<Txn.Kind> KEY_KINDS = Gens.pick(ALLOWED_KEY_KINDS);
    public static final Gen<Gen<Txn.Kind>> KEY_KINDS_DISTRIBUTION = Gens.mixedDistribution(ALLOWED_KEY_KINDS);
    public static final List<Txn.Kind> ALLOWED_RANGE_KINDS = ImmutableList.of(Txn.Kind.Read, Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint);
    public static final Gen<Txn.Kind> RANGE_KINDS = Gens.pick(ALLOWED_RANGE_KINDS);
    public static final Gen<Gen<Txn.Kind>> RANGE_KINDS_DISTRIBUTION = Gens.mixedDistribution(ALLOWED_RANGE_KINDS);

    private Gen<Routable.Domain> domainGen = DOMAIN_GEN;
    private Gen<Keys> keysGen = AccordGens.keys(AccordGens.intKeys());
    private Gen<Txn.Kind> keyKinds = KEY_KINDS;
    private Gen<Ranges> rangesGen = AccordGens.ranges(Gens.ints().between(1, 5), AccordGens.intRoutingKey(), (ignore, a, b) -> IntKey.range(a, b));
    private Gen<Txn.Kind> rangeKinds = RANGE_KINDS;
    private TxnFactory txnFactory = ListTxnFactory.instance;

    public TxnGenBuilder()
    {
    }

    public TxnGenBuilder(RandomSource rs)
    {
        domainGen = DOMAIN_DISTRIBUTION.next(rs);
        keyKinds = KEY_KINDS_DISTRIBUTION.next(rs);
        rangeKinds = RANGE_KINDS_DISTRIBUTION.next(rs);
    }

    public TxnGenBuilder withKeys(Gen<Keys> keysGen)
    {
        this.keysGen = keysGen;
        return this;
    }

    public TxnGenBuilder mapKeys(Function<? super Keys, ? extends Keys> fn)
    {
        return withKeys(keysGen.map(fn));
    }

    public TxnGenBuilder withRanges(Gen<Ranges> rangesGen)
    {
        this.rangesGen = rangesGen;
        return this;
    }

    public TxnGenBuilder mapRanges(Function<? super Ranges, ? extends Ranges> fn)
    {
        return withRanges(rangesGen.map(fn));
    }

    public TxnGenBuilder withTxnFactory(TxnFactory txnFactory)
    {
        this.txnFactory = txnFactory;
        return this;
    }

    public Gen<Txn> build()
    {
        return rs -> {
            Seekables<?, ?> keysOrRanges;
            Txn.Kind kind;
            switch (domainGen.next(rs))
            {
                case Key:
                {
                    keysOrRanges = keysGen.next(rs);
                    kind = keyKinds.next(rs);
                }
                break;
                case Range:
                {
                    keysOrRanges = rangesGen.next(rs);
                    kind = rangeKinds.next(rs);
                }
                break;
                default:
                    throw new UnsupportedOperationException();
            }
            switch (kind)
            {
                case Read:
                    return txnFactory.read(rs, keysOrRanges);
                case Write:
                {
                    Invariants.checkArgument(keysOrRanges.domain() == Routable.Domain.Key, "Only key txn may do writes");
                    Seekables<?, ?> readKeysOrRanges = rs.nextBoolean() ? keysOrRanges : Keys.EMPTY;
                    return txnFactory.write(rs, readKeysOrRanges, keysOrRanges);
                }
                case ExclusiveSyncPoint:
                case SyncPoint:
                    return txnFactory.syncPoint(rs, kind, keysOrRanges);
                default:
                    throw new UnsupportedOperationException(kind.name());
            }
        };
    }

    public interface TxnFactory
    {
        Txn read(RandomSource rs, Seekables<?, ?> keysOrRanges);

        Txn write(RandomSource rs, Seekables<?, ?> readKeys, Seekables<?, ?> writeKeys);

        Txn syncPoint(RandomSource rs, Txn.Kind kind, Seekables<?, ?> keysOrRanges);
    }

    public static class ListTxnFactory implements TxnFactory
    {
        private static final ListAgent AGENT = Mockito.mock(ListAgent.class, Mockito.CALLS_REAL_METHODS);

        public static final ListTxnFactory instance = new ListTxnFactory();

        @Override
        public Txn read(RandomSource rs, Seekables<?, ?> keysOrRanges)
        {
            ListRead read = new ListRead(Function.identity(), false, keysOrRanges, keysOrRanges);
            ListQuery query = new ListQuery(null, 42, false);
            return new Txn.InMemory(keysOrRanges, read, query);
        }

        @Override
        public Txn write(RandomSource rs, Seekables<?, ?> readKeysOrRanges, Seekables<?, ?> writeKeysOrRanges)
        {
            Invariants.checkArgument(readKeysOrRanges.domain() == Routable.Domain.Key, "Only key txn may do writes");
            Invariants.checkArgument(writeKeysOrRanges.domain() == Routable.Domain.Key, "Only key txn may do writes");

            ListRead read = new ListRead(Function.identity(), false, readKeysOrRanges, readKeysOrRanges);
            ListQuery query = new ListQuery(null, 42, false);
            ListUpdate update = new ListUpdate(Function.identity());
            for (Key key : (Keys) writeKeysOrRanges)
                update.put(key, 1);
            return new Txn.InMemory(writeKeysOrRanges, read, query, update);
        }

        @Override
        public Txn syncPoint(RandomSource rs, Txn.Kind kind, Seekables<?, ?> keysOrRanges)
        {
            return AGENT.emptyTxn(kind, keysOrRanges);
        }
    }
}
