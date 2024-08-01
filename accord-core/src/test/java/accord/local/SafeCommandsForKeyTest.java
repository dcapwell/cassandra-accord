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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.junit.jupiter.api.Test;

import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemorySafeCommand;
import accord.impl.InMemorySafeCommandsForKey;
import accord.impl.InMemorySafeTimestampsForKey;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.list.ListData;
import accord.impl.list.ListStore;
import accord.local.CommandTransformation.NamedCommandTransformation;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
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
import accord.utils.Property;
import accord.utils.Property.Commands;
import accord.utils.Property.SimpleCommand;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static accord.local.CommandTransformation.Result.done;
import static accord.local.CommandTransformation.Result.ok;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.utils.Property.stateful;

class SafeCommandsForKeyTest
{
    private static Property.Command<State, Void, ?> addTxn(CommandUpdator updator)
    {
        return new SimpleCommand<>("Add Txn " + updator.txnId + "; " + updator.txn, state -> state.pendingTxns.put(updator.txnId, updator));
    }

    private static Property.Command<State, Void, ?> commandStep(CommandUpdator updator)
    {
        return new SimpleCommand<>("Next Step for " + updator.txnId + ": " + updator.name(), state -> {
            if (!updator.processNext(state.safeStore))
                state.pendingTxns.remove(updator.txnId);
        });
    }

    private static Gen<Property.Command<State, Void, ?>> createTxn(State state)
    {
        return rs -> {
            Txn txn = state.txnGen.next(rs);
            TxnId id = state.nextTxnId(txn);
            CommandUpdator updator = new CommandUpdator(id, txn, Iterators.peekingIterator(transformationsGen(id, txn).next(rs).iterator()));
            return Property.multistep(addTxn(updator), commandStep(updator));
        };
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
                if (state.pendingTxns.isEmpty())
                    return createTxn(state);
                return rs -> {
                    // pick a txn to move forward
                    TxnId id = rs.pickUnorderedSet(state.pendingTxns.keySet());
                    CommandUpdator update = state.pendingTxns.get(id);
                    return commandStep(update);
                };
            }
        });
        // public void update(SafeCommandStore safeStore, Command nextCommand)
        // public void registerHistorical(SafeCommandStore safeStore, TxnId txnId)
        // public void updateRedundantBefore(SafeCommandStore safeStore, RedundantBefore.Entry redundantBefore)

        //TODO
        // void updatePruned(SafeCommandStore safeStore, Command nextCommand, NotifySink notifySink)
        // void registerUnmanaged(SafeCommandStore safeStore, SafeCommand unmanaged)
    }

    private static class State
    {
        private static final Node.Id NODE = new Node.Id(42);
        private final Map<TxnId, CommandUpdator> pendingTxns = new HashMap<>();
        private final SafeCommandStore safeStore;
        private final Key key;
        private final long epoch = 1;
        private final Topology topology = new Topology(epoch, new Shard(IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE),
                                                                        Collections.singletonList(NODE),
                                                                        Collections.singleton(NODE)));
        private final AtomicLong time = new AtomicLong(0);
        private final LongSupplier clock;
        private final Gen<Txn> txnGen;

        State(RandomSource rs)
        {
            key = AccordGens.intKeys().next(rs);
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
            ProgressLog.Factory factory = ignore -> Mockito.mock(ProgressLog.class);
            CommandStore.EpochUpdateHolder epochHolder = new CommandStore.EpochUpdateHolder();
            Ranges ranges = Ranges.of(IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE));
            ListStore listStore = new ListStore(State.NODE);
            Node node = Mockito.mock(Node.class);
            Mockito.when(node.id()).thenReturn(NODE);
            listStore.onTopologyUpdate(node, topology);
            InMemoryCommandStore store = new InMemoryCommandStore(0, timeService, new TestAgent.RethrowAgent(), listStore, factory, epochHolder)
            {
                @Override
                public boolean inStore()
                {
                    return true;
                }

                @Override
                public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
                {
                    return submit(context, ss -> {
                        consumer.accept(ss);
                        return null;
                    });
                }

                @Override
                public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply)
                {
                    return new AsyncChains.Head<>()
                    {

                        @Override
                        protected void start(BiConsumer<? super T, Throwable> callback)
                        {
                            T value;
                            try
                            {
                                value = apply.apply(safeStore);
                            }
                            catch (Throwable t)
                            {
                                callback.accept(null, t);
                                return;
                            }
                            callback.accept(value, null);
                        }
                    };
                }

                @Override
                public void shutdown()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> AsyncChain<T> submit(Callable<T> task)
                {
                    return submit(PreLoadContext.empty(), ignore -> {
                        try
                        {
                            return task.call();
                        }
                        catch (Exception e)
                        {
                            throw new RuntimeException(e);
                        }
                    });
                }
            };
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
            TxnGenBuilder txnBuilder = new TxnGenBuilder();
            txnBuilder.mapKeys(keys -> keys.with(key));
            txnBuilder.mapRanges(r -> {
                if (r.contains(key))
                    return r;
                return r.with(Ranges.of(key.asRange()));
            });
            txnGen = txnBuilder.build();
        }

        TxnId nextTxnId(Txn txn)
        {
            return new TxnId(epoch, clock.getAsLong(), txn.kind(), txn.keys().domain(), NODE);
        }
    }

    private static Gen<List<NamedCommandTransformation>> transformationsGen(TxnId txnId, Txn txn)
    {
        Gen<CheckedCommands.Messages> firstMessageGen = Gens.enums().all(CheckedCommands.Messages.class);
        Function<SafeCommandStore, Keys> keysForCFK = safeStore -> {
            // txn can be key or range txn, where they have slightly different semantics
            List<Key> keys = safeStore.mapReduceFull(txn.keys(), safeStore.ranges().allAt(txnId), txnId,
                                                     AnyGloballyVisible,
                                                     SafeCommandStore.TestStartedAt.ANY,
                                                     SafeCommandStore.TestDep.ANY_DEPS,
                                                     SafeCommandStore.TestStatus.ANY_STATUS,
                                                     (ignore, keyOrRange, id, executeAt, accum) -> {
                                                         if (keyOrRange.domain().isKey())
                                                             accum.add(keyOrRange.asKey());
                                                         return accum;
                                                     }, null, new ArrayList<>());
            return Keys.of(keys.toArray(Key[]::new));
        };
        return rs -> {
            List<NamedCommandTransformation> ts = new ArrayList<>();
            if (rs.decide(0.02))
            {
                ts.add(new NamedCommandTransformation("Register Historical", cs -> {
                    for (Key key : keysForCFK.apply(cs))
                    {
                        // adding historic command
                        cs.get(key).registerHistorical(cs, txnId);
                    }
                    return ok(null);
                }));
            }
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
                return partial;
            };
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
                                            data.put(key, ((ListStore) cs.dataStore()).get(Ranges.EMPTY, txnId, key));
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
                        return done(cs.get(txnId).current());
                    }));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            return ts;
        };
    }

    private static class CommandUpdator
    {
        final TxnId txnId;
        final Txn txn;
        final PeekingIterator<NamedCommandTransformation> transformations;

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
            Assertions.assertThat(transformations.hasNext()).isTrue();
            NamedCommandTransformation next = transformations.next();
            CommandTransformation.Result result = next.transform(safeStore);
            switch (result.status)
            {
                case Ok:
                    return true;
                case Done:
                    return false;
                default:
                    throw new UnsupportedOperationException(result.status.name());
            }
        }
    }
}