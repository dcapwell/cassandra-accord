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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.Infer;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemorySafeCommand;
import accord.impl.InMemorySafeCommandsForKey;
import accord.impl.InMemorySafeTimestampsForKey;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.list.ListData;
import accord.impl.list.ListStore;
import accord.impl.list.ListWrite;
import accord.local.CommandTransformation.NamedCommandTransformation;
import accord.messages.CheckStatus;
import accord.messages.PreAccept;
import accord.messages.Propagate;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnGenBuilder;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.Property;
import accord.utils.Property.Commands;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static accord.local.CommandTransformation.Result.done;
import static accord.local.CommandTransformation.Result.ignore;
import static accord.local.CommandTransformation.Result.ok;
import static accord.utils.Property.stateful;
import static org.assertj.core.api.Assertions.assertThat;

class SafeCommandsForKeyTest
{
    private static final Logger logger = LoggerFactory.getLogger(SafeCommandsForKeyTest.class);

    private static final List<SaveStatus> FINAL_STATUSES = Stream.of(SaveStatus.values()).filter(s -> s.ordinal() >= SaveStatus.Applied.ordinal() && s.ordinal() < SaveStatus.ErasedOrInvalidated.ordinal()).collect(Collectors.toList());
    public static final Range FULL_RANGE = IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE);

    private static Property.Command<State, Void, ?> addTxn(CommandUpdator updator, boolean historic)
    {
        return new SimpleCommand<>("Add "+(historic ? "historic " : "")+"Txn " + updator.txnId + "; " + updator.txn, state -> state.pendingTxns.put(updator.txnId, updator));
    }

    private static Property.Command<State, Void, ?> commandStep(CommandUpdator updator)
    {
        return new SimpleCommand<>("Next Step for " + updator.txnId + ": " + updator.name(), state -> {
            if (!updator.processNext(state.safeStore))
            {
                state.pendingTxns.remove(updator.txnId);
                state.finishedTxns.add(updator.txnId);
            }
        });
    }

    private static Gen<Property.Command<State, Void, ?>> createTxn(State state)
    {
        return rs -> createUpdator(state, rs, false);
    }

    private static Property.MultistepCommand<State, Void> createUpdator(State state, RandomSource rs, boolean historic)
    {
        Txn txn = state.txnGen.next(rs);
        TxnId id = state.nextTxnId(txn);
        if (historic)
            id = id.withEpoch(id.epoch() - 1);
        CommandUpdator updator = new CommandUpdator(id, txn, Iterators.peekingIterator(transformationsGen(id, txn, historic).next(rs).iterator()));
        return Property.multistep(addTxn(updator, historic), commandStep(updator));
    }

    @Test
    void test()
    {
        stateful().withSeed(-4825193645054929911L).check(new Commands<State, Void>()
        {
            @Override
            public Gen<State> genInitialState()
            {
                return State::new;
            }

            @Override
            public Void createSut(State state)
            {
                return null;
            }

            @Override
            public Gen<Property.Command<State, Void, ?>> commands(State state)
            {
                state.checkErrors();
                //Rules:
                // CFK.manages -> Key + Read/Write/SyncPoint (called already)
                // CFK.managesExecution -> Key + Read/Write (called already)
                // SafeCFK.registerUnmanaged -> Range + Read/SyncPoint/ExclusiveSyncPoint (called already)
                // SafeCFK.registerHistorical -> Key + Read/Write/SyncPoint (called already)
                // SafeCFK.updatePruned -> ??? (called already)
                // CommandStore.markBootstraping - before sync point (called already)
                // CommandStore.markSharedDurable - after sync point (called already)
                // CommandStore.markShardStale - ???
                // CommandStore.markExclusiveSyncPointLocallyApplied - RX applied locally (called already)
                // CommandStore.add|remove - topology change (called already)
                if (state.numHistoricTxns > 0)
                {
                    return rs -> {
                        List<Property.Command<State, Void, ?>> commands = new ArrayList<>(state.numHistoricTxns);
                        for (int i = 0; i < state.numHistoricTxns; i++)
                            commands.add(createUpdator(state, rs, true));
                        state.numHistoricTxns = -1 * state.numHistoricTxns;
                        return Property.multistep(commands);
                    };
                }
                if (state.pendingTxns.isEmpty())
                    return createTxn(state);

                Order o = pendingOrder(state);
                Map<Gen<Property.Command<State, Void, ?>>, Integer> possible = new LinkedHashMap<>();
                possible.put(createTxn(state), 1);
                if (!o.anyOrder.isEmpty())
                {
                    possible.put(rs -> commandStep(state.pendingTxns.get(rs.pick(o.anyOrder))), 1);
                }
                if (!o.sequential.isEmpty())
                {
                    possible.put(ignore -> commandStep(state.pendingTxns.get(o.sequential.get(0))), 1);
                }
                return Gens.oneOf(possible);
            }

            @Override
            public void destroyState(State state)
            {
                state.checkErrors();
                // make sure all transactions complete...
                for (int attempt = 0; attempt < 100 && !state.pendingTxns.isEmpty(); attempt++)
                {
                    // what does CFK think?
                    CommandUpdator blockingCFK = state.blockingCFK();
                    if (blockingCFK != null)
                    {
                        if (!blockingCFK.processNext(state.safeStore))
                        {
                            state.pendingTxns.remove(blockingCFK.txnId);
                            state.finishedTxns.add(blockingCFK.txnId);
                        }
                        continue;
                    }

                    Order o = pendingOrder(state);
                    if (!o.anyOrder.isEmpty())
                    {
                        CommandUpdator updator = state.pendingTxns.get(o.anyOrder.get(0));
                        if (!updator.processNext(state.safeStore))
                        {
                            state.pendingTxns.remove(updator.txnId);
                            state.finishedTxns.add(updator.txnId);
                        }
                    }
                    else if (!o.sequential.isEmpty())
                    {
                        CommandUpdator updator = state.pendingTxns.get(o.sequential.get(0));
                        logger.info("Trying to run txn {}", updator.txnId);
                        if (!updator.processNext(state.safeStore))
                        {
                            state.pendingTxns.remove(updator.txnId);
                            state.finishedTxns.add(updator.txnId);
                        }
                    }

                    // what is the txn state
                    Map<TxnId, SaveStatus> inflight = new TreeMap<>();
                    for (TxnId id : new ArrayList<>(state.finishedTxns))
                    {
                        Command cmd = state.safeStore.get(id).current();
                        if (cmd.hasBeen(Status.Applied))
                        {
                            state.finishedTxns.remove(id);
                            continue;
                        }
                        inflight.put(id, cmd.saveStatus());
                    }
                    if (!inflight.isEmpty())
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Inflight Transactions:\n");
                        for (Map.Entry<TxnId, SaveStatus> e : inflight.entrySet())
                            sb.append("\t").append(e.getKey()).append(": ").append(e.getValue()).append("\n");
                        logger.warn(sb.toString());
                    }
                    CommandsForKey cfk = state.safeStore.get(state.key).current();
                    if (cfk.size() != 0)
                    {
                        CommandsForKey.TxnInfo info = cfk.get(0);
                        logger.info("CommandsForKey waiting on {}", info);
                    }
                }
                assertThat(state.pendingTxns.isEmpty()).isTrue();
                state.checkErrors();
            }
        });
    }

    private static class Order
    {
        final List<TxnId> anyOrder, sequential;

        private Order(List<TxnId> anyOrder, List<TxnId> sequential)
        {
            this.anyOrder = anyOrder;
            this.sequential = sequential;
        }
    }
    private static Order pendingOrder(State state)
    {
        List<TxnId> anyOrder = new ArrayList<>();
        List<TxnId> sequential = new ArrayList<>();
        // find all txn that can get to ReadyToExecute, they can run in any order
        // find the oldest ReadyToExecute and apply
        for (CommandUpdator txn : state.pendingTxns.values())
        {
            if (txn.current == null)
            {
                anyOrder.add(txn.txnId);
                continue;
            }
            if (!txn.current.hasBeen(Status.Stable))
            {
                anyOrder.add(txn.txnId);
                continue;
            }
            sequential.add(txn.txnId);
        }
        Invariants.checkState(!(anyOrder.isEmpty() && sequential.isEmpty()));
        anyOrder.sort(Comparator.naturalOrder());
        sequential.sort(Comparator.naturalOrder());
        return new Order(anyOrder, sequential);
    }

    private static class State
    {
        private static final Node.Id NODE = new Node.Id(42);
        private final TreeMap<TxnId, CommandUpdator> pendingTxns = new TreeMap<>();
        private final Set<TxnId> finishedTxns = new HashSet<>();
        private final SafeCommandStore safeStore;
        private final Key key;
        private final long epoch = 2;
        private final Topology topology = new Topology(epoch, new Shard(IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE),
                                                                        Collections.singletonList(NODE),
                                                                        Collections.singleton(NODE)));
        private final AtomicLong time = new AtomicLong(0);
        private final LongSupplier clock;
        private final Gen<Txn> txnGen;
        private int numHistoricTxns;
        private final List<Throwable> errors = new ArrayList<>();

        State(RandomSource rs)
        {
            key = AccordGens.intKeys().next(rs);
            numHistoricTxns = rs.nextInt(0, 10);
            {
                RandomSource fork = rs.fork();
                clock = () -> {
                    long now = time.get();
                    long jitter = fork.nextInt(0, 1024);
                    long hlc = now + 1 + jitter;
                    time.set(hlc);
                    return hlc;
                };
            }
            NodeTimeService timeService = new NodeTimeService()
            {
                @Override
                public Node.Id id()
                {
                    return NODE;
                }

                @Override
                public long epoch()
                {
                    return epoch;
                }

                @Override
                public long now()
                {
                    return clock.getAsLong();
                }

                @Override
                public long unix(TimeUnit unit)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Timestamp uniqueNow(Timestamp atLeast)
                {
                    throw new UnsupportedOperationException();
                }
            };
            ProgressLog.Factory factory = ignore -> ProgressLog.noop();
            CommandStore.EpochUpdateHolder epochHolder = new CommandStore.EpochUpdateHolder();
            Ranges ranges = Ranges.of(IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE));
            ListStore listStore = new ListStore(State.NODE);
            Node node = Mockito.mock(Node.class);
            Mockito.when(node.id()).thenReturn(NODE);
            listStore.onTopologyUpdate(node, topology);
            TestAgent agent = new TestAgent() {
                @Override
                public void onUncaughtException(Throwable t)
                {
                    errors.add(t);
                }

                @Override
                public void onHandledException(Throwable t)
                {
                    errors.add(t);
                }
            };
            InMemoryCommandStore store = new InMemoryCommandStore.Synchronized(0, timeService, agent, listStore, factory, epochHolder);
            epochHolder.add(epoch, new CommandStores.RangesForEpoch(epoch, ranges, store), ranges);
            safeStore = new InMemoryCommandStore.InMemorySafeStore(store, store.updateRangesForEpoch(), PreLoadContext.contextFor(key), new HashMap<>(), new HashMap<>(), new HashMap<>())
            {
                @Override
                protected InMemorySafeCommand getCommandInternal(TxnId txnId)
                {
                    InMemorySafeCommand cmd = super.getCommandInternal(txnId);
                    if (cmd == null)
                    {
                        cmd = new InMemorySafeCommand(txnId, store.command(txnId));
                        addCommandInternal(cmd);
                    }
                    return cmd;
                }

                @Override
                protected InMemorySafeCommandsForKey getCommandsForKeyInternal(Key key)
                {
                    InMemorySafeCommandsForKey cfk = super.getCommandsForKeyInternal(key);
                    if (cfk == null)
                    {
                        cfk = new InMemorySafeCommandsForKey(key, store.commandsForKey(key));
                        addCommandsForKeyInternal(cfk);
                        if (cfk.isUnset())
                            cfk.initialize();
                    }
                    return cfk;
                }

                @Override
                protected InMemorySafeTimestampsForKey getTimestampsForKeyInternal(Key key)
                {
                    InMemorySafeTimestampsForKey cfk = super.getTimestampsForKeyInternal(key);
                    if (cfk == null)
                    {
                        cfk = new InMemorySafeTimestampsForKey(key, store.timestampsForKey(key));
                        addTimestampsForKeyInternal(cfk);
                        if (cfk.isUnset())
                            cfk.initialize();
                    }
                    return cfk;
                }

                @Override
                public boolean canExecuteWith(PreLoadContext context)
                {
                    return true;
                }
            };
            TxnGenBuilder txnBuilder = new TxnGenBuilder(rs);
            txnBuilder.withKeys(ignore -> Keys.of(key)).withRanges(ignore -> Ranges.single(key.asRange()));
            txnGen = txnBuilder.build();
        }

        TxnId nextTxnId(Txn txn)
        {
            return new TxnId(epoch, clock.getAsLong(), txn.kind(), txn.keys().domain(), NODE);
        }

        void checkErrors()
        {
            if (!errors.isEmpty())
            {
                AssertionError error = new AssertionError("Unhandled errors detected");
                errors.forEach(error::addSuppressed);
            }
        }

        public CommandUpdator blockingCFK()
        {
            CommandsForKey cfk = safeStore.get(key).current();
            for (int i = 0, size = cfk.size(); i < size; i++)
            {
                CommandsForKey.TxnInfo info = cfk.get(i);
                if (info.status.compareTo(CommandsForKey.InternalStatus.APPLIED) < 0)
                {
                    // do we have an updator
                    TxnId id = new TxnId(info);
                    logger.warn("Txn blocking CFK: {} / {}", info, safeStore.get(id).current());
                    return pendingTxns.get(id);
                }
            }
            return null;
        }
    }

    private static Gen<List<NamedCommandTransformation>> transformationsGen(TxnId txnId, Txn txn, boolean historic)
    {
        Gen<CheckedCommands.Messages> firstMessageGen = Gens.enums().all(CheckedCommands.Messages.class);
        FullRoute<?> fullRoute = txn.keys().domain() == Routable.Domain.Key ?
                                 txn.keys().toRoute(((Keys) txn.keys()).get(0).toUnseekable()) :
                                 txn.keys().toRoute(((Ranges) txn.keys()).get(0).end());
        AtomicReference<PartialDeps> depsRef = new AtomicReference<>();
        Function<SafeCommandStore, PartialDeps> getDeps = safeStore -> {
            PartialDeps partial = depsRef.get();
            if (partial == null)
            {
                partial = PreAccept.calculatePartialDeps(safeStore, txnId, txn.keys(), txnId, txnId, safeStore.ranges().allAt(txnId));
                depsRef.set(partial);
            }
            // simulate load by reloading the commands
            for (TxnId id : partial.txnIds())
                safeStore.get(id);
            return partial;
        };
        return rs -> {
            List<NamedCommandTransformation> ts = new ArrayList<>();
            if (historic)
            {
                SaveStatus saveStatus = rs.pick(FINAL_STATUSES);
                ts.add(new NamedCommandTransformation("Register Historical", cs -> {
                    Deps.Builder builder = Deps.builder();
                    txn.keys().forEach(s -> builder.add(s, txnId));
                    cs.registerHistoricalTransactions(builder.build());
                    return ok(null);
                }));
                ts.add(new NamedCommandTransformation("Historic Propagate", cs -> {
                    PartialTxn partialTxn = !saveStatus.known.isDefinitionKnown() ? null : txn.slice(Ranges.single(FULL_RANGE), true);
                    PartialDeps partialDeps = !saveStatus.known.deps.hasProposedOrDecidedDeps() ? null : PartialDeps.NONE;
                    Writes writes = saveStatus.known.outcome.compareTo(Status.Outcome.WasApply) < 0 ? null : new Writes(txnId, txnId, Keys.EMPTY, new ListWrite(Function.identity()));
                    Result result = saveStatus.known.outcome.compareTo(Status.Outcome.WasApply) < 0 ? null : txn.query().compute(txnId, txnId, Keys.EMPTY, null, null, null);
                    CheckStatus.FoundKnownMap knownMap = CheckStatus.FoundKnownMap.create(txn.keys().toParticipants(), saveStatus, Infer.InvalidIfNot.IfUnknown, Ballot.ZERO);
                    Propagate propagate = Propagate.SerializerSupport.create(txnId, fullRoute, saveStatus, saveStatus, Ballot.ZERO,
                                                                             Status.Durability.MajorityOrInvalidated,
                                                                             fullRoute.homeKey(), fullRoute.homeKey(),
                                                                             saveStatus.known,
                                                                             knownMap,
                                                                             false,
                                                                             partialTxn, partialDeps,
                                                                             txnId.epoch(), txnId, writes, result);
                    SaveStatus previous;
                    Command current;
                    do
                    {
                        previous = cs.get(txnId, txnId, fullRoute).current().saveStatus();
                        propagate.apply(cs);
                        current = cs.get(txnId, txnId, fullRoute).current();
                    }
                    while (!current.hasBeen(Status.Applied) && previous != current.saveStatus());
                    // This is here for debugging only...
                    if (!current.hasBeen(Status.Applied))
                        propagate.apply(cs);
//                    Assertions.assertThat(current.hasBeen(Status.Applied)).describedAs("%s is not %s", current, saveStatus).isTrue();
                    return done(current);
                }));
                return ts;
            }
            boolean isExclusiveSyncPoint = txnId.kind() == Txn.Kind.ExclusiveSyncPoint;
            boolean isBootstrap = isExclusiveSyncPoint && rs.nextBoolean();
            if (isExclusiveSyncPoint && isBootstrap)
            {
                ts.add(new NamedCommandTransformation("markBootstraping", cs -> {
                    cs.commandStore().markBootstrapping(cs, txnId, (Ranges) txn.keys());
                    return ok(cs.get(txnId).current());
                }));
            }
            switch (firstMessageGen.next(rs))
            {
                case PreAccept:
                    ts.add(new NamedCommandTransformation("PreAccept", cs -> {
                        PartialTxn partialTxn = txn.slice(cs.ranges().currentRanges(), true);
                        CheckedCommands.preaccept(cs, txnId, partialTxn, fullRoute, fullRoute.homeKey());
                        return ok(cs.get(txnId).current());
                    }));
                case Accept:
                    ts.add(new NamedCommandTransformation("Accept", cs -> {
                        PartialRoute<?> route = fullRoute.slice(cs.ranges().currentRanges());
                        PartialDeps partialDeps = getDeps.apply(cs);
                        CheckedCommands.accept(cs, txnId, Ballot.ZERO, route, txn.keys(), fullRoute.homeKey(), txnId, partialDeps);
                        return ok(cs.get(txnId).current());
                    }));
                case Commit:
                    ts.add(new NamedCommandTransformation("Commit", cs -> {
                        PartialTxn partialTxn = txn.slice(cs.ranges().currentRanges(), true);
                        PartialDeps partialDeps = getDeps.apply(cs);
                        CheckedCommands.commit(cs, SaveStatus.Stable, Ballot.ZERO, txnId, fullRoute, fullRoute.homeKey(), partialTxn, txnId, partialDeps);
                        return ok(cs.get(txnId).current());
                    }));
                case Apply:
                    ts.add(new NamedCommandTransformation("Apply", cs -> {
                        PartialTxn partialTxn = txn.slice(cs.ranges().currentRanges(), true);
                        PartialDeps partialDeps = getDeps.apply(cs);
                        Command current = cs.get(txnId).current();
                        if (current.hasBeen(Status.Stable) && current.asCommitted().waitingOn.isWaiting())
                        {
                            if (current.asCommitted().waitingOn.isWaitingOnKey())
                            {
                                CommandsForKey cfk = cs.get(current.asCommitted().waitingOn.keys.get(0)).current();
                                CommandsForKey.TxnInfo waitingOn = null;
                                Command waitingOnCommand = null;
                                for (int i = 0, size = cfk.size(); i < size; i++)
                                {
                                    waitingOn = cfk.get(i);
                                    waitingOnCommand = cs.get(new TxnId(waitingOn)).current();
                                    if (!waitingOnCommand.hasBeen(Status.Applied))
                                        break;
                                }
                                logger.warn("Txn {} is blocked by CommandsForKey: {} ({}) / {}", current, waitingOn, waitingOnCommand, cfk);
                            }
                            return ignore(current);
                        }
                        Assertions.assertThat(current.saveStatus()).describedAs("Txn %s is not ready to execute...", current).isIn(SaveStatus.Uninitialised, SaveStatus.ReadyToExecute);
                        ListData data;
                        try
                        {
                            switch (txnId.kind())
                            {
                                case SyncPoint:
                                case ExclusiveSyncPoint:
                                    data = null;
                                    break;
                                default:
                                {
                                    data = (ListData) AsyncChains.getBlocking(txn.read(cs, txnId, Ranges.EMPTY));
                                    if (data == null)
                                        data = new ListData();
                                    // this test does not require that writes must first do a read... but ListUpdate does, we still need reads to exist...
                                    if (txn.update() != null)
                                    {
                                        Keys keys = (Keys) txn.update().keys();
                                        keys = keys.subtract((Keys) txn.read().keys());
                                        for (Key key : keys)
                                            data.put(key, ((ListStore) cs.dataStore()).get(Ranges.EMPTY, TxnId.NONE, key));
                                    }
                                }
                            }
                        }
                        catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                            throw new UncheckedInterruptedException(e);
                        }
                        catch (ExecutionException e)
                        {
                            throw new RuntimeException(e.getCause());
                        }
                        Result result = txn.result(txnId, txnId, data);
                        Writes writes = txn.execute(txnId, txnId, data);
                        CheckedCommands.apply(cs, txnId, fullRoute, fullRoute.homeKey(), txnId, partialDeps, partialTxn, writes, result);
                        if (!(isExclusiveSyncPoint && !isBootstrap))
                            return done(cs.get(txnId).current());
                        else
                            return ok(cs.get(txnId).current());
                    }));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            if (isExclusiveSyncPoint && !isBootstrap)
            {
                ts.add(new NamedCommandTransformation("markSharedDurable", cs -> {
                    Command starting = cs.get(txnId).current();
                    if (!starting.hasBeen(Status.Applied))
                        return ignore(starting);
                    cs.commandStore().markShardDurable(cs, txnId, (Ranges) txn.keys());
                    return done(cs.get(txnId).current());
                }));
            }
            return ts;
        };
    }

    private static class CommandUpdator
    {
        final TxnId txnId;
        final Txn txn;
        final PeekingIterator<NamedCommandTransformation> transformations;
        @Nullable
        Command current = null;

        private CommandUpdator(TxnId txnId, Txn txn, PeekingIterator<NamedCommandTransformation> transformations)
        {
            this.txnId = txnId;
            this.txn = txn;
            this.transformations = transformations;
        }

        String name()
        {
            return transformations.peek().name;
        }

        boolean processNext(SafeCommandStore safeStore)
        {
            assertThat(transformations.hasNext()).isTrue();
            NamedCommandTransformation next = transformations.peek();
            CommandTransformation.Result result = next.transform(safeStore);
            current = result.command;
            switch (result.status)
            {
                case Ok:
                    transformations.next();
                    return true;
                case Done:
                    transformations.next();
                    return false;
                case Ignore:
                    return true;
                default:
                    throw new UnsupportedOperationException(result.status.name());
            }
        }
    }
}