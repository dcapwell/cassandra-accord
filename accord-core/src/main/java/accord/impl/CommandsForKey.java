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

package accord.impl;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Key;

import accord.local.Command;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.local.SaveStatus;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedList;

import static accord.impl.CommandsForKey.InternalStatus.STABLE;
import static accord.impl.CommandsForKey.InternalStatus.HISTORICAL;
import static accord.impl.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED;
import static accord.impl.CommandsForKey.InternalStatus.PROPOSED;
import static accord.impl.CommandsForKey.InternalStatus.TRANSITIVELY_KNOWN;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.illegalState;

public class CommandsForKey implements CommandsSummary
{
    public static final TxnId[] NO_TXNIDS = new TxnId[0];
    public static final Info[] NO_INFOS = new Info[0];

    public static class SerializerSupport
    {
        public static CommandsForKey create(Key key, TxnId redundantBefore, TxnId[] txnIds, Info[] infos)
        {
            return new CommandsForKey(key, redundantBefore, txnIds, infos);
        }
    }

    public enum InternalStatus
    {
        TRANSITIVELY_KNOWN(false, false), // (unwitnessed, no need for other transactions to witness)
        HISTORICAL(false, false),
        PREACCEPTED(false),
        PROPOSED(true),
        STABLE(true),
        PREAPPLIED(true),
        INVALID_OR_TRUNCATED(false);

        static final EnumMap<SaveStatus, InternalStatus> convert = new EnumMap<>(SaveStatus.class);
        static final InternalStatus[] VALUES = values();
        static
        {
            convert.put(SaveStatus.PreAccepted, PREACCEPTED);
            convert.put(SaveStatus.AcceptedInvalidateWithDefinition, PREACCEPTED);
            convert.put(SaveStatus.Accepted, PROPOSED);
            convert.put(SaveStatus.AcceptedWithDefinition, PROPOSED);
            convert.put(SaveStatus.PreCommittedWithDefinition, PREACCEPTED);
            convert.put(SaveStatus.PreCommittedWithAcceptedDeps, PROPOSED);
            convert.put(SaveStatus.PreCommittedWithDefinitionAndAcceptedDeps, PROPOSED);
            convert.put(SaveStatus.Committed, PROPOSED);
            convert.put(SaveStatus.Stable, STABLE);
            convert.put(SaveStatus.ReadyToExecute, STABLE);
            convert.put(SaveStatus.PreApplied, PREAPPLIED);
            convert.put(SaveStatus.Applying, PREAPPLIED);
            convert.put(SaveStatus.Applied, PREAPPLIED);
            convert.put(SaveStatus.TruncatedApplyWithDeps, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.TruncatedApplyWithOutcome, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.TruncatedApply, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.ErasedOrInvalidated, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.Erased, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.Invalidated, INVALID_OR_TRUNCATED);
        }

        public final boolean hasInfo;
        public final NoInfo asNoInfo;
        public final InfoAndAdditions asNoInfoOrAdditions;
        final InternalStatus expectMatch;

        InternalStatus(boolean hasInfo)
        {
            this(hasInfo, true);
        }

        InternalStatus(boolean hasInfo, boolean expectMatch)
        {
            this.hasInfo = hasInfo;
            this.asNoInfo = new NoInfo(this);
            this.asNoInfoOrAdditions = new InfoAndAdditions(asNoInfo, NO_TXNIDS, 0);
            this.expectMatch = expectMatch ? this : null;
        }

        boolean hasExecuteAt()
        {
            return hasInfo;
        }

        boolean hasDeps()
        {
            return hasInfo;
        }

        static InternalStatus from(SaveStatus status)
        {
            return convert.get(status);
        }

        public static InternalStatus get(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    public static class Info
    {
        public final InternalStatus status;
        // DO NOT ACCESS DIRECTLY: use accessor method to ensure correct value is returned
        public final @Nullable Timestamp executeAt;
        public final TxnId[] missing; // those TxnId we know of that would be expected to be found in the provided deps, but aren't

        private Info(InternalStatus status, @Nullable Timestamp executeAt, TxnId[] missing)
        {
            this.status = status;
            this.executeAt = executeAt;
            this.missing = missing;
        }

        public static Info create(@Nonnull TxnId txnId, InternalStatus status, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing)
        {
            return new Info(status,
                            Invariants.checkArgument(executeAt, executeAt == txnId || executeAt.compareTo(txnId) >= 0),
                            Invariants.checkArgument(missing, missing == NO_TXNIDS || missing.length > 0));
        }

        public static Info createMock(InternalStatus status, @Nullable Timestamp executeAt, TxnId[] missing)
        {
            return new Info(status, executeAt, Invariants.checkArgument(missing, missing == null || missing == NO_TXNIDS));
        }

        Timestamp executeAt(TxnId txnId)
        {
            return executeAt;
        }

        Info update(TxnId txnId, TxnId[] newMissing)
        {
            return newMissing == NO_TXNIDS && executeAt == txnId ? status.asNoInfo : new Info(status, executeAt, newMissing);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Info info = (Info) o;
            return status == info.status && Objects.equals(executeAt, info.executeAt) && Arrays.equals(missing, info.missing);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "Info{" +
                   "status=" + status +
                   ", executeAt=" + executeAt +
                   ", missing=" + Arrays.toString(missing) +
                   '}';
        }
    }

    public static class NoInfo extends Info
    {
        NoInfo(InternalStatus status)
        {
            super(status, null, NO_TXNIDS);
        }

        Timestamp executeAt(TxnId txnId)
        {
            return status.hasExecuteAt() ? txnId : executeAt;
        }
    }

    private final Key key;
    private final TxnId redundantBefore;
    private final TxnId[] txnIds;
    private final Info[] infos;

    CommandsForKey(Key key, TxnId redundantBefore, TxnId[] txnIds, Info[] infos)
    {
        this.key = key;
        this.redundantBefore = redundantBefore;
        this.txnIds = txnIds;
        this.infos = infos;
        Invariants.checkArgument(txnIds.length == 0 || infos[infos.length - 1] != null);
    }

    public CommandsForKey(Key key)
    {
        this.key = key;
        this.redundantBefore = TxnId.NONE;
        this.txnIds = NO_TXNIDS;
        this.infos = NO_INFOS;
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    public Key key()
    {
        return key;
    }

    public int size()
    {
        return txnIds.length;
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId);
    }

    public TxnId txnId(int i)
    {
        return txnIds[i];
    }

    public Info info(int i)
    {
        return infos[i];
    }

    public TxnId redundantBefore()
    {
        return redundantBefore;
    }

    /**
     * All commands before/after (exclusive of) the given timestamp
     * <p>
     * Note that {@code testDep} applies only to commands that MAY have the command in their deps; if specified any
     * commands that do not know any deps will be ignored, as will any with an executeAt prior to the txnId.
     * <p>
     */
    public <P1, T> T mapReduceFull(TxnId testTxnId,
                                   Kinds testKind,
                                   TestStartedAt testStartedAt,
                                   TestDep testDep,
                                   TestStatus testStatus,
                                   CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        int start, end;
        boolean isKnown;
        {
            int insertPos = Arrays.binarySearch(txnIds, testTxnId);
            isKnown = insertPos >= 0;
            if (!isKnown && testDep == WITH) return initialValue;
            if (!isKnown) insertPos = -1 - insertPos;
            switch (testStartedAt)
            {
                default: throw new AssertionError("Unhandled TestStartedAt: " + testTxnId);
                case STARTED_BEFORE: start = 0; end = insertPos; break;
                case STARTED_AFTER: start = insertPos; end = txnIds.length; break;
                case ANY: start = 0; end = txnIds.length;
            }
        }

        for (int i = start; i < end ; ++i)
        {
            TxnId txnId = txnIds[i];
            if (!testKind.test(txnId.kind())) continue;

            Info info = infos[i];
            InternalStatus status = info.status;
            switch (testStatus)
            {
                default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                case IS_PROPOSED:
                    if (status == PROPOSED) break;
                    else continue;
                case IS_STABLE:
                    if (status.compareTo(STABLE) >= 0 && status.compareTo(INVALID_OR_TRUNCATED) < 0) break;
                    else continue;
                case ANY_STATUS:
                    if (status == TRANSITIVELY_KNOWN)
                        continue;
            }

            Timestamp executeAt = info.executeAt(txnId);
            if (testDep != ANY_DEPS)
            {
                if (!status.hasInfo)
                    continue;

                if (executeAt.compareTo(testTxnId) <= 0)
                    continue;

                boolean hasAsDep = Arrays.binarySearch(info.missing, testTxnId) < 0;
                if (hasAsDep != (testDep == WITH))
                    continue;
            }

            initialValue = map.apply(p1, key, txnId, executeAt, initialValue);
        }
        return initialValue;
    }

    public <P1, T> T mapReduceActive(Timestamp startedBefore,
                                     Kinds testKind,
                                     CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        int start = 0, end = insertPos(startedBefore);

        for (int i = start; i < end ; ++i)
        {
            TxnId txnId = txnIds[i];
            if (!testKind.test(txnId.kind()))
                continue;

            Info info = infos[i];
            if (info.status == TRANSITIVELY_KNOWN)
                continue;

            initialValue = map.apply(p1, key, txnId, info.executeAt(txnId), initialValue);
        }
        return initialValue;
    }

    public CommandsForKey update(Command prev, Command next)
    {
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            return this;

        TxnId txnId = next.txnId();
        int pos = Arrays.binarySearch(txnIds, txnId);
        if (pos < 0)
        {
            pos = -1 - pos;
            if (!newStatus.hasInfo)
                return insert(pos, txnId, newStatus.asNoInfo);

            return insert(pos, txnId, computeInfoAndAdditions(pos, txnId, newStatus, next));
        }
        else
        {
            // update
            InternalStatus prevStatus = prev == null ? null : InternalStatus.from(prev.saveStatus());
            if (newStatus == prevStatus && (!newStatus.hasInfo || next.acceptedOrCommitted().equals(prev.acceptedOrCommitted())))
                return this;

            @Nonnull Info cur = Invariants.nonNull(infos[pos]);
            Invariants.checkState(cur.status.expectMatch == prevStatus);

            if (newStatus.hasInfo) return update(pos, txnId, computeInfoAndAdditions(pos, txnId, newStatus, next));
            else return update(pos, cur, newStatus.asNoInfo);
        }
    }

    public static boolean needsUpdate(Command prev, Command updated)
    {
        if (!updated.txnId().kind().isGloballyVisible())
            return false;

        SaveStatus prevStatus;
        Ballot prevAcceptedOrCommitted;
        if (prev == null)
        {
            prevStatus = SaveStatus.NotDefined;
            prevAcceptedOrCommitted = Ballot.ZERO;
        }
        else
        {
            prevStatus = prev.saveStatus();
            prevAcceptedOrCommitted = prev.acceptedOrCommitted();
        }

        return needsUpdate(prevStatus, prevAcceptedOrCommitted, updated.saveStatus(), updated.acceptedOrCommitted());
    }

    public static boolean needsUpdate(SaveStatus prevStatus, Ballot prevAcceptedOrCommitted, SaveStatus updatedStatus, Ballot updatedAcceptedOrCommitted)
    {
        InternalStatus prev = InternalStatus.from(prevStatus);
        InternalStatus updated = InternalStatus.from(updatedStatus);
        return updated != prev || (updated != null && updated.hasInfo && !prevAcceptedOrCommitted.equals(updatedAcceptedOrCommitted));
    }

    private CommandsForKey insert(int pos, TxnId txnId, InfoAndAdditions newInfo)
    {
        if (newInfo.additionCount == 0)
            return insert(pos, txnId, newInfo.info);

        TxnId[] newTxnIds = new TxnId[txnIds.length + newInfo.additionCount + 1];
        Info[] newInfos = new Info[newTxnIds.length];
        int insertPos = insertWithAdditions(pos, txnId, newInfo, newTxnIds, newInfos);
        // TODO (desired): we do some redundant copying here; there might be a better way to share code
        insertInfoAndMissing(insertPos, txnId, newInfo.info, newInfos, newInfos, newTxnIds, 0);
        return new CommandsForKey(key, redundantBefore, newTxnIds, newInfos);
    }

    private CommandsForKey update(int pos, TxnId txnId, InfoAndAdditions newInfo)
    {
        if (newInfo.additionCount == 0)
            return update(pos, newInfo.info);

        TxnId[] newTxnIds = new TxnId[txnIds.length + newInfo.additionCount];
        Info[] newInfos = new Info[newTxnIds.length];
        updateWithAdditions(pos, txnId, newInfo, newTxnIds, newInfos);
        return new CommandsForKey(key, redundantBefore, newTxnIds, newInfos);
    }

    private int updateWithAdditions(int sourceInsertPos, TxnId updateTxnId, InfoAndAdditions newInfo, TxnId[] newTxnIds, Info[] newInfos)
    {
        return updateOrInsertWithAdditions(sourceInsertPos, sourceInsertPos, updateTxnId, newInfo, newTxnIds, newInfos);
    }

    private int insertWithAdditions(int sourceInsertPos, TxnId updateTxnId, InfoAndAdditions newInfo, TxnId[] newTxnIds, Info[] newInfos)
    {
        return updateOrInsertWithAdditions(-1, sourceInsertPos, updateTxnId, newInfo, newTxnIds, newInfos);
    }

    private int updateOrInsertWithAdditions(int sourceUpdatePos, int sourceInsertPos, TxnId updateTxnId, InfoAndAdditions newInfo, TxnId[] newTxnIds, Info[] newInfos)
    {
        int additionInsertPos = Arrays.binarySearch(newInfo.additions, 0, newInfo.additionCount, updateTxnId);
        additionInsertPos = Invariants.checkArgument(-1 - additionInsertPos, additionInsertPos < 0);
        int targetInsertPos = sourceInsertPos + additionInsertPos;

        int i = 0, j = 0, count = 0;
        while (i < infos.length && j < newInfo.additionCount)
        {
            if (count == targetInsertPos)
            {
                newTxnIds[count] = updateTxnId;
                newInfos[count] = newInfo.info;
                if (txnIds[i].equals(updateTxnId)) ++i;
            }
            else
            {
                int c = txnIds[i].compareTo(newInfo.additions[j]);
                if (c < 0)
                {
                    newTxnIds[count] = txnIds[i];
                    newInfos[count] = infos[i];
                    i++;
                }
                else if (c > 0)
                {
                    newTxnIds[count] = newInfo.additions[j++];
                    newInfos[count] = TRANSITIVELY_KNOWN.asNoInfo;
                }
                else illegalState(txnIds[i] + " should be an insertion, but found match when merging with origin");
            }
            count++;
        }

        if (i < infos.length)
        {
            if (count <= targetInsertPos)
            {
                int length = targetInsertPos - count;
                System.arraycopy(txnIds, i, newTxnIds, count, length);
                System.arraycopy(infos, i, newInfos, count, length);
                newTxnIds[targetInsertPos] = updateTxnId;
                newInfos[targetInsertPos] = newInfo.info;
                count = targetInsertPos + 1;
                i = sourceUpdatePos >= 0 ? sourceInsertPos + 1 : sourceInsertPos;
            }
            System.arraycopy(txnIds, i, newTxnIds, count, txnIds.length - i);
            System.arraycopy(infos, i, newInfos, count, txnIds.length - i);
        }
        else if (j < newInfo.additionCount)
        {
            if (count <= targetInsertPos)
            {
                int length = targetInsertPos - count;
                System.arraycopy(newInfo.additions, j, newTxnIds, count, length);
                Arrays.fill(newInfos, count, targetInsertPos, TRANSITIVELY_KNOWN.asNoInfo);
                newTxnIds[targetInsertPos] = updateTxnId;
                newInfos[targetInsertPos] = newInfo.info;
                count = targetInsertPos + 1;
                j = additionInsertPos;
            }
            System.arraycopy(newInfo.additions, j, newTxnIds, count, newInfo.additionCount - j);
            Arrays.fill(newInfos, count, count + newInfo.additionCount - j, TRANSITIVELY_KNOWN.asNoInfo);
        }

        cachedTxnIds().forceDiscard(newInfo.additions, newInfo.additionCount);
        return targetInsertPos;
    }

    private CommandsForKey update(int pos, Info curInfo, Info newInfo)
    {
        if (curInfo == newInfo)
            return this;
        return update(pos, newInfo);
    }

    private CommandsForKey update(int pos, Info newInfo)
    {
        Info[] newInfos = infos.clone();
        newInfos[pos] = newInfo;
        return new CommandsForKey(key, redundantBefore, txnIds, newInfos);
    }

    private CommandsForKey insert(int pos, TxnId txnId, Info newInfo)
    {
        TxnId[] newTxnIds = new TxnId[txnIds.length + 1];
        System.arraycopy(txnIds, 0, newTxnIds, 0, pos);
        newTxnIds[pos] = txnId;
        System.arraycopy(txnIds, pos, newTxnIds, pos + 1, txnIds.length - pos);

        Info[] newInfos = new Info[infos.length + 1];
        insertInfoAndMissing(pos, txnId, newInfo, infos, newInfos, newTxnIds, 1);
        return new CommandsForKey(key, redundantBefore, newTxnIds, newInfos);

    }

    private static void insertInfoAndMissing(int insertPos, TxnId txnId, Info newInfo, Info[] oldInfos, Info[] newInfos, TxnId[] newTxnIds, int offsetAfterInsertPos)
    {
        TxnId[] oneMissing = null;
        for (int i = 0 ; i < insertPos ; ++i)
        {
            Info oldInfo = oldInfos[i];
            if (oldInfo.getClass() == NoInfo.class || oldInfo.executeAt.compareTo(txnId) <= 0)
            {
                newInfos[i] = oldInfo;
                continue;
            }

            TxnId[] missing;
            if (oldInfo.missing == NO_TXNIDS)
                missing = oneMissing = ensureOneMissing(txnId, oneMissing);
            else
                missing = SortedArrays.insert(oldInfo.missing, txnId, TxnId[]::new);

            newInfos[i] = Info.create(txnId, oldInfo.status, oldInfo.executeAt, missing);
        }

        newInfos[insertPos] = newInfo;

        for (int i = insertPos; i < oldInfos.length ; ++i)
        {
            Info oldInfo = oldInfos[i];
            if (!oldInfo.status.hasDeps())
            {
                newInfos[i + offsetAfterInsertPos] = oldInfo;
                continue;
            }

            TxnId[] missing;
            if (oldInfo.missing == NO_TXNIDS)
                missing = oneMissing = ensureOneMissing(txnId, oneMissing);
            else
                missing = SortedArrays.insert(oldInfo.missing, txnId, TxnId[]::new);

            int newIndex = i + offsetAfterInsertPos;
            newInfos[newIndex] = Info.create(newTxnIds[newIndex], oldInfo.status, oldInfo.executeAt(newTxnIds[newIndex]), missing);
        }
    }

    private static TxnId[] ensureOneMissing(TxnId txnId, TxnId[] oneMissing)
    {
        return oneMissing != null ? oneMissing : new TxnId[] { txnId };
    }

    static class InfoAndAdditions
    {
        final Info info;
        final TxnId[] additions;
        final int additionCount;

        InfoAndAdditions(Info info, TxnId[] additions, int additionCount)
        {
            this.info = info;
            this.additions = additions;
            this.additionCount = additionCount;
        }
    }

    private InfoAndAdditions computeInfoAndAdditions(int pos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        return computeInfoAndAdditions(pos, txnId, newStatus, command.executeAt(), command.partialDeps().keyDeps.txnIds(key));
    }

    private InfoAndAdditions computeInfoAndAdditions(int pos, TxnId txnId, InternalStatus newStatus, Timestamp executeAt, SortedList<TxnId> deps)
    {
        int executeAtPos;
        if (executeAt.equals(txnId))
        {
            executeAt = txnId;
            executeAtPos = pos;
        }
        else
        {
            executeAtPos = Arrays.binarySearch(txnIds, pos, txnIds.length, executeAt);
            Invariants.checkState(executeAtPos < 0);
            executeAtPos = -1 - executeAtPos;
        }

        TxnId[] additions = NO_TXNIDS, missing = NO_TXNIDS;
        int additionCount = 0, missingCount = 0;

        int depsIndex = deps.find(redundantBefore);
        if (depsIndex < 0) depsIndex = -1 - depsIndex;
        int txnIdsIndex = 0;
        while (txnIdsIndex < executeAtPos && depsIndex < deps.size())
        {
            TxnId t = txnIds[txnIdsIndex];
            TxnId d = deps.get(depsIndex);
            int c = t.compareTo(d);
            if (c == 0)
            {
                ++txnIdsIndex;
                ++depsIndex;
            }
            else if (c < 0)
            {
                if (missingCount == missing.length)
                    missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                missing[missingCount++] = t;
                txnIdsIndex++;
            }
            else
            {
                if (additionCount == additions.length)
                    additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));
                additions[additionCount++] = d;
                depsIndex++;
            }
        }

        while (txnIdsIndex < executeAtPos)
        {
            TxnId t = txnIds[txnIdsIndex++];
            if (missingCount == missing.length)
                missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
            missing[missingCount++] = t;
        }
        while (depsIndex < deps.size())
        {
            TxnId d = deps.get(depsIndex++);
            if (additionCount == additions.length)
                additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));
            additions[additionCount++] = d;
        }

        if (missingCount == 0 && executeAt == txnId)
            return additionCount == 0 ? newStatus.asNoInfoOrAdditions : new InfoAndAdditions(newStatus.asNoInfo, additions, additionCount);

        return new InfoAndAdditions(Info.create(txnId, newStatus, executeAt, cachedTxnIds().completeAndDiscard(missing, missingCount)), additions, additionCount);
    }

    public CommandsForKey withoutRedundant(TxnId redundantBefore)
    {
        if (this.redundantBefore.compareTo(redundantBefore) >= 0)
            return this;

        int pos = insertPos(redundantBefore);
        if (pos == 0)
            return new CommandsForKey(key, redundantBefore, txnIds, infos);

        TxnId[] newTxnIds = Arrays.copyOfRange(txnIds, pos, txnIds.length);
        Info[] newInfos = Arrays.copyOfRange(infos, pos, infos.length);
        for (int i = 0 ; i < newInfos.length ; ++i)
        {
            Info info = newInfos[i];
            if (info.getClass() == NoInfo.class) continue;
            if (info.missing == NO_TXNIDS) continue;
            int j = Arrays.binarySearch(info.missing, redundantBefore);
            if (j < 0) j = -1 - i;
            if (j <= 0) continue;
            TxnId[] newMissing = j == info.missing.length ? NO_TXNIDS : Arrays.copyOfRange(info.missing, j, info.missing.length);
            newInfos[i] = info.update(txnIds[i], newMissing);
        }
        return new CommandsForKey(key, redundantBefore, newTxnIds, newInfos);
    }

    public CommandsForKey registerHistorical(TxnId txnId)
    {
        int i = Arrays.binarySearch(txnIds, txnId);
        if (i >= 0)
            return infos[i].status.compareTo(HISTORICAL) >= 0 ? this : update(i, infos[i], HISTORICAL.asNoInfo);

        return insert(-1 - i, txnId, HISTORICAL.asNoInfo);
    }

    private int insertPos(Timestamp timestamp)
    {
        int i = Arrays.binarySearch(txnIds, timestamp);
        if (i < 0) i = -1 -i;
        return i;
    }

    public TxnId findFirst()
    {
        return txnIds.length > 0 ? txnIds[0] : null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return Objects.equals(key, that.key) && Objects.equals(redundantBefore, that.redundantBefore) && Arrays.equals(txnIds, that.txnIds) && Arrays.equals(infos, that.infos);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
