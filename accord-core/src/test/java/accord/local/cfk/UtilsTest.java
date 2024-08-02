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

package accord.local.cfk;

import java.util.Arrays;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.TxnInfoExtra;
import accord.primitives.Ballot;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

class UtilsTest
{
    private static final Gen<TxnId> KW_ID_GEN = AccordGens.txnIds(i -> Txn.Kind.Write, i -> Routable.Domain.Key);
    private static final Gen<TxnId[]> IDS_GEN = Gens.arrays(TxnId.class, KW_ID_GEN).unique().ofSizeBetween(1, 10).map(a -> {
        Arrays.sort(a);
        return a;
    });
    private static final Gen<TxnInfo> INFO_GEN = rs -> {
        TxnId txnId = KW_ID_GEN.next(rs);
        CommandsForKey.InternalStatus status = rs.pick(CommandsForKey.InternalStatus.values());
        Timestamp executeAt = txnId; //TODO (coverage): randomize
        return new TxnInfo(txnId, status, executeAt);
    };
    private static final Gen<TxnInfoExtra> EXTRA_INFO_GEN = rs -> {
        TxnId txnId = KW_ID_GEN.next(rs);
        CommandsForKey.InternalStatus status = rs.pick(CommandsForKey.InternalStatus.values());
        Timestamp executeAt = txnId; //TODO (coverage): randomize
        TxnId[] missing = IDS_GEN.next(rs);
        Ballot ballot = AccordGens.ballot().next(rs);
        return new TxnInfoExtra(txnId, status, executeAt, missing, ballot);
    };
    private static final Gen<TxnInfo[]> BY_ID_GEN = Gens.arrays(TxnInfo.class, Gens.oneOf(INFO_GEN, EXTRA_INFO_GEN)).unique().ofSizeBetween(1, 10).map(a -> {
        Arrays.sort(a);
        return a;
    });

    @Test
    void removeOneMissing()
    {
        qt().forAll(IDS_GEN).check(missing -> {
            for (TxnId id : missing)
            {
                assertThat(Utils.removeOneMissing(missing, id)).hasSize(missing.length - 1)
                                                               .doesNotContain(id);
            }
        });
    }

    @Test
    void removeRedundantMissing()
    {
        qt().forAll(IDS_GEN).check(missing -> {
            int i = 0;
            for (TxnId id : missing)
            {
                assertThat(Utils.removeRedundantMissing(missing, id)).isEqualTo(Arrays.copyOfRange(missing, i++, missing.length));
            }
        });
    }

    @Test
    void removeFromMissingArraysById()
    {
        qt().forAll(Gens.random(), BY_ID_GEN).check((rs, byId) -> {
            TreeSet<TxnId> allMissing = allMissing(byId);
            if (allMissing.isEmpty()) return;;
            TxnId candidate = rs.pickOrderedSet(allMissing);
            Utils.removeFromMissingArraysById(byId, 0, byId.length, candidate);

            assertThat(allMissing(byId)).describedAs("Missing still included %s", candidate).isEqualTo(Sets.filter(allMissing, a -> a != candidate));
        });
    }

    private static TreeSet<TxnId> allMissing(TxnInfo[] byId)
    {
        TreeSet<TxnId> allMissing = new TreeSet<>();
        for (var a : byId)
        {
            if (!(a instanceof TxnInfoExtra)) continue;
            if (!a.status.hasExecuteAtOrDeps) continue;
            if (a.status != CommandsForKey.InternalStatus.ACCEPTED) continue;
            allMissing.addAll(Arrays.asList(a.missing()));
        }
        return allMissing;
    }
}