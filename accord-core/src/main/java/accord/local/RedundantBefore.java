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

package accord.local;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.local.RedundantStatus.LIVE;
import static accord.local.RedundantStatus.NOT_OWNED;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.REDUNDANT;
import static accord.local.RedundantStatus.nonNullOrMin;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Entry>
{
    public static class SerializerSupport
    {
        public static RedundantBefore create(boolean inclusiveEnds, RoutingKey[] ends, Entry[] values)
        {
            return new RedundantBefore(inclusiveEnds, ends, values);
        }
    }

    public static class Entry
    {
        public final Range range;
        public final long startEpoch, endEpoch;
        public final @Nonnull TxnId redundantBefore, bootstrappedAt;

        public Entry(Range range, long startEpoch, long endEpoch, @Nonnull TxnId redundantBefore, @Nonnull TxnId bootstrappedAt)
        {
            this.range = range;
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.redundantBefore = redundantBefore;
            this.bootstrappedAt = bootstrappedAt;
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry a, Entry b)
        {
            long startEpoch = Long.max(a.startEpoch, b.startEpoch);
            long endEpoch = Long.min(a.endEpoch, b.endEpoch);
            int cr = a.redundantBefore.compareTo(b.redundantBefore);

            TxnId redundantBefore = cr >= 0 ? a.redundantBefore : b.redundantBefore;

            if (range.equals(a.range) && startEpoch == a.startEpoch && endEpoch == a.endEpoch && cr >= 0)
                return a;
            if (range.equals(b.range) && startEpoch == b.startEpoch && endEpoch == b.endEpoch && cr <= 0)
                return b;

            return new Entry(range, startEpoch, endEpoch, redundantBefore, Timestamp.max(a.bootstrappedAt, b.bootstrappedAt));
        }

        static RedundantStatus mergeMin(Entry entry, RedundantStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (prev == LIVE || entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            return nonNullOrMin(prev, entry.get(txnId));
        }

        static RedundantStatus mergeLocallyRedundant(Entry entry, RedundantStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (prev == REDUNDANT || entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            return nonNullOrMin(prev, entry.get(txnId));
        }

        static RedundantStatus mergeRedundant(Entry entry, RedundantStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            // TODO (expected): this is identical to mergeLocallyRedundant
            if (prev == REDUNDANT || entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            return nonNullOrMin(prev, entry.get(txnId));
        }

        RedundantStatus get(TxnId txnId)
        {
            if (redundantBefore.compareTo(txnId) > 0)
                return REDUNDANT;
            if (bootstrappedAt.compareTo(txnId) > 0)
                return PRE_BOOTSTRAP;
            return LIVE;
        }

        public final boolean isLocalRedundancyViaBootstrap()
        {
            return redundantBefore.compareTo(bootstrappedAt) < 0;
        }

        public final TxnId locallyRedundantBefore()
        {
            return TxnId.min(bootstrappedAt, redundantBefore);
        }

        // TODO (required, consider): this admits the range of epochs that cross the two timestamps, which matches our
        //   behaviour elsewhere but we probably want to only interact with the two point epochs in which we participate,
        //   but note hasRedundantDependencies which really does want to scan a range
        private boolean outOfBounds(Timestamp lb, EpochSupplier ub)
        {
            return ub.epoch() < startEpoch || lb.epoch() >= endEpoch;
        }

        Entry withRange(Range range)
        {
            return new Entry(range, startEpoch, endEpoch, redundantBefore, bootstrappedAt);
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return this.range.equals(that.range) && equalsIgnoreRange(that);
        }

        public boolean equalsIgnoreRange(Entry that)
        {
            return this.startEpoch == that.startEpoch
                   && this.endEpoch == that.endEpoch
                   && this.redundantBefore.equals(that.redundantBefore);
        }

        @Override
        public String toString()
        {
            return "("
                   + (startEpoch == Long.MIN_VALUE ? "-\u221E" : Long.toString(startEpoch)) + ","
                   + (endEpoch == Long.MAX_VALUE ? "\u221E" : Long.toString(endEpoch)) + ","
                   + (redundantBefore.compareTo(bootstrappedAt) >= 0 ? redundantBefore + ")" : bootstrappedAt + "*)");
        }
    }

    public static RedundantBefore EMPTY = new RedundantBefore();

    private RedundantBefore()
    {
    }

    RedundantBefore(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values)
    {
        super(inclusiveEnds, starts, values);
    }

    public static RedundantBefore create(Ranges ranges, long startEpoch, long endEpoch, @Nonnull TxnId redundantBefore, @Nonnull TxnId bootstrappedAt)
    {
        if (ranges.isEmpty())
            return new RedundantBefore();

        Entry entry = new Entry(null, startEpoch, endEpoch, redundantBefore, bootstrappedAt);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), entry.withRange(cur), (a, b) -> { throw new IllegalStateException(); });
            builder.append(cur.end(), null, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.merge(a, b, RedundantBefore.Entry::reduce, Builder::new);
    }

    public RedundantStatus min(TxnId txnId, @Nullable EpochSupplier executeAt, Routables<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return notOwnedIfNull(foldl(participants, Entry::mergeMin, null, txnId, executeAt, test -> test == LIVE));
    }

    public RedundantStatus get(TxnId txnId, @Nullable EpochSupplier executeAt, RoutingKey participant)
    {
        if (executeAt == null) executeAt = txnId;
        Entry entry = get(participant);
        return entry == null ? NOT_OWNED : notOwnedIfNull(Entry.mergeMin(entry,null, txnId, executeAt));
    }

    public boolean isLocallyRedundant(TxnId txnId, Timestamp executeAt, Participants<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        // essentially, if ANY intersecting key is REDUNDANT then this command has either applied locally or is invalidated;
        // otherwise, if ALL intersecting keys are PRE_BOOTSTRAP then any all of its effects will be reflected in the respective bootstraps that
        return notOwnedIfNull(foldl(participants, Entry::mergeLocallyRedundant, null, txnId, executeAt, test -> test == REDUNDANT)).compareTo(PRE_BOOTSTRAP) >= 0;
    }

    public boolean isRedundant(TxnId txnId, Timestamp executeAt, Participants<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        // TODO (expected): this calls mergeLocallyRedundant instead of mergeRedundant
        // essentially, if ANY intersecting key is REDUNDANT then this command has either applied locally or is invalidated;
        // otherwise, if ALL intersecting keys are PRE_BOOTSTRAP then any all of its effects will be reflected in the respective bootstraps that
        return notOwnedIfNull(foldl(participants, Entry::mergeLocallyRedundant, null, txnId, executeAt, test -> test == REDUNDANT)).compareTo(PRE_BOOTSTRAP) >= 0;
    }

    private static RedundantStatus notOwnedIfNull(RedundantStatus status)
    {
        return status == null ? NOT_OWNED : status;
    }

    static class Builder extends ReducingIntervalMap.Builder<RoutingKey, Entry, RedundantBefore>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected boolean equals(Entry a, Entry b)
        {
            return a.equalsIgnoreRange(b);
        }

        @Override
        protected Entry mergeEqual(Entry a, Entry b)
        {
            Invariants.checkState(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Entry(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startEpoch, a.endEpoch, a.redundantBefore, a.bootstrappedAt);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }
}
