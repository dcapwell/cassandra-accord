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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

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
import accord.impl.list.ListAgent;
import accord.impl.list.ListData;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListStore;
import accord.impl.list.ListUpdate;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.Property;
import accord.utils.Property.Commands;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;

import static accord.local.SafeCommandsForKeyTest.CommandTransformation.Result.done;
import static accord.local.SafeCommandsForKeyTest.CommandTransformation.Result.ok;
import static accord.utils.Property.stateful;

class SafeCommandsForKeyTest
{
    private static Property.Command<State, Void, ?> addTxn(CommandUpdator updator)
    {
        return new SimpleCommand("Add Txn " + updator.txnId, state -> state.pendingTxns.put(updator.txnId, updator));
    }

    private static Property.Command<State, Void, ?> commandStep(CommandUpdator updator)
    {
        return new SimpleCommand("Next Step for " + updator.txnId + ": " + updator.name(), state -> {
            Assertions.assertThat(updator.transformations.hasNext()).isTrue();
            CommandTransformation next = updator.transformations.next();
            CommandTransformation.Result result = next.transform(state.safeStore, state.cfk, updator.current);
            updator.current = result.command;
            if (result.status == CommandTransformation.Status.Done)
                state.pendingTxns.remove(updator.txnId);
        });
    }

    private static Gen<Property.Command<State, Void, ?>> createTxn(State state)
    {
        return rs -> {
            TxnId id = state.idGen.next(rs);
            CommandUpdator updator = new CommandUpdator(id, Iterators.peekingIterator(transformationsGen(id, state.key).next(rs).iterator()));
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
        private final InMemorySafeCommandsForKey cfk;
        private final AtomicLong time = new AtomicLong(0);
        private final Gen<TxnId> idGen = rs -> {
            long now = time.get();
            long jitter = rs.nextInt(0, 1024);
            long hlc = now + 1 + jitter;
            time.set(hlc);
            Routable.Domain domain = TxnGenBuilder.DOMAIN_GEN.next(rs);
            Txn.Kind kind = (domain == Routable.Domain.Key ? TxnGenBuilder.KEY_KINDS : TxnGenBuilder.RANGE_KINDS).next(rs);
            return new TxnId(1, hlc, kind, domain, NODE);
        };

        State(RandomSource rs)
        {
            key = AccordGens.intKeys().next(rs);
            cfk = new InMemorySafeCommandsForKey(key, new InMemoryCommandStore.GlobalCommandsForKey(key));
            NodeTimeService timeService = new NodeTimeService()
            {
                @Override
                public Node.Id id()
                {
                    return null;
                }

                @Override
                public long epoch()
                {
                    return 0;
                }

                @Override
                public long now()
                {
                    return 0;
                }

                @Override
                public long unix(TimeUnit unit)
                {
                    return 0;
                }

                @Override
                public Timestamp uniqueNow(Timestamp atLeast)
                {
                    return null;
                }
            };
            ProgressLog.Factory factory = ignore -> Mockito.mock(ProgressLog.class);
            CommandStore.EpochUpdateHolder epochHolder = new CommandStore.EpochUpdateHolder();
            Ranges ranges = Ranges.of(IntKey.range(Integer.MIN_VALUE, Integer.MAX_VALUE));
            InMemoryCommandStore store = new InMemoryCommandStore(0, timeService, new TestAgent.RethrowAgent(), new ListStore(State.NODE), factory, epochHolder)
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
            epochHolder.add(1, new CommandStores.RangesForEpoch(1, ranges, store), ranges);
            safeStore = new InMemoryCommandStore.InMemorySafeStore(store, store.updateRangesForEpoch(), PreLoadContext.contextFor(key), new HashMap<>(), new HashMap<>(), new HashMap<>())
            {
                @Override
                protected InMemorySafeCommand getCommandInternal(TxnId txnId)
                {
                    InMemorySafeCommand cmd = super.getCommandInternal(txnId);
                    if (cmd == null)
                    {
                        cmd = new InMemorySafeCommand(txnId, new InMemoryCommandStore.GlobalCommand(txnId));
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
                        cfk = key.equals(State.this.key)? State.this.cfk : new InMemorySafeCommandsForKey(key, new InMemoryCommandStore.GlobalCommandsForKey(key));
                        addCommandsForKeyInternal(cfk);
                    }
                    return cfk;
                }

                @Override
                protected InMemorySafeTimestampsForKey getTimestampsForKeyInternal(Key key)
                {
                    InMemorySafeTimestampsForKey cfk = super.getTimestampsForKeyInternal(key);
                    if (cfk == null)
                    {
                        cfk = new InMemorySafeTimestampsForKey(key, new InMemoryCommandStore.GlobalTimestampsForKey(key));
                        addTimestampsForKeyInternal(cfk);
                    }
                    return cfk;
                }

                @Override
                public boolean canExecuteWith(PreLoadContext context)
                {
                    return true;
                }
            };
        }
    }

    private static Gen<List<SimpleCommandTransformation>> transformationsGen(TxnId txnId, Key key)
    {
        Gen<CheckedCommands.Messages> firstMessageGen = Gens.enums().all(CheckedCommands.Messages.class);
        return rs -> {
            List<SimpleCommandTransformation> ts = new ArrayList<>();
            if (rs.decide(0.02))
            {
                ts.add(new SimpleCommandTransformation("Register Historical", (cs, cfk, ignore) -> {
                    // adding historic command
                    cfk.registerHistorical(cs, txnId);
                    return ok(null);
                }));
            }
            TxnGenBuilder txnBuilder = new TxnGenBuilder();
            txnBuilder.mapKeys(keys -> keys.with(key));
            txnBuilder.mapRanges(ranges -> {
                if (ranges.contains(key))
                    return ranges;
                return ranges.with(Ranges.of(key.asRange()));
            });
            Txn txn = txnBuilder.forTxnId(txnId).build().next(rs);
            FullRoute<?> fullRoute = txn.keys().domain() == Routable.Domain.Key ?
                                     txn.keys().toRoute(((Keys) txn.keys()).get(0).toUnseekable()) :
                                     txn.keys().toRoute(((Ranges) txn.keys()).get(0).end());
            Deps deps = Deps.NONE;
            switch (firstMessageGen.next(rs))
            {
                case PreAccept:
                    ts.add(new SimpleCommandTransformation("PreAccept", (cs, cfk, cmd) -> {
                        PartialTxn partialTxn = txn.slice(cs.ranges().currentRanges(), true);
                        CheckedCommands.preaccept(cs, txnId, partialTxn, fullRoute, fullRoute.homeKey());
                        return ok(cs.get(txnId).current());
                    }));
                case Accept:
                    ts.add(new SimpleCommandTransformation("Accept", (cs, cfk, cmd) -> {
                        PartialRoute<?> route = fullRoute.slice(cs.ranges().currentRanges());
                        PartialDeps partialDeps = deps.slice(cs.ranges().currentRanges());
                        CheckedCommands.accept(cs, txnId, Ballot.ZERO, route, txn.keys(), fullRoute.homeKey(), txnId, partialDeps);
                        return ok(cs.get(txnId).current());
                    }));
                case Commit:
                    ts.add(new SimpleCommandTransformation("Commit", (cs, cfk, cmd) -> {
                        PartialTxn partialTxn = txn.slice(cs.ranges().currentRanges(), true);
                        PartialDeps partialDeps = deps.slice(cs.ranges().currentRanges());
                        CheckedCommands.commit(cs, SaveStatus.Stable, Ballot.ZERO, txnId, fullRoute, fullRoute.homeKey(), partialTxn, txnId, partialDeps);
                        return ok(cs.get(txnId).current());
                    }));
                case Apply:
                    ts.add(new SimpleCommandTransformation("Apply", (cs, cfk, cmd) -> {
                        PartialTxn partialTxn = txn.slice(cs.ranges().currentRanges(), true);
                        PartialDeps partialDeps = deps.slice(cs.ranges().currentRanges());
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
                                    data = (ListData) AsyncChains.getBlocking(txn.read(cs, txnId, Ranges.EMPTY));
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

    public static class TxnGenBuilder
    {
        public static final Gen<Routable.Domain> DOMAIN_GEN = Gens.enums().all(Routable.Domain.class);
        public static final Gen<Txn.Kind> KEY_KINDS = Gens.pick(Txn.Kind.Write, Txn.Kind.Read, Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint);
        public static final Gen<Txn.Kind> RANGE_KINDS = Gens.pick(Txn.Kind.Read, Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint);

        ListAgent agent = Mockito.mock(ListAgent.class, Mockito.CALLS_REAL_METHODS);
        Gen<Routable.Domain> domainGen = DOMAIN_GEN;
        Gen<Keys> keysGen = AccordGens.keys(AccordGens.intKeys());
        Gen<Txn.Kind> keyKinds = KEY_KINDS;
        Gen<Ranges> rangesGen = AccordGens.ranges(Gens.ints().between(1, 5), AccordGens.intRoutingKey(), (ignore, a, b) -> IntKey.range(a, b));
        Gen<Txn.Kind> rangeKinds = RANGE_KINDS;

        TxnGenBuilder mapKeys(Function<? super Keys, ? extends Keys> fn)
        {
            keysGen = keysGen.map(fn);
            return this;
        }

        TxnGenBuilder mapRanges(Function<? super Ranges, ? extends Ranges> fn)
        {
            rangesGen = rangesGen.map(fn);
            return this;
        }

        TxnGenBuilder forTxnId(TxnId id)
        {
            domainGen = ignore -> id.domain();
            keyKinds = rangeKinds = ignore -> id.kind();
            return this;
        }

        Gen<Txn> build()
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
                    {
                        ListRead read = new ListRead(Function.identity(), false, keysOrRanges, keysOrRanges);
                        ListQuery query = new ListQuery(null, 42, false);
                        return new Txn.InMemory(kind, keysOrRanges, read, query, null);
                    }
                    case Write:
                    {
                        Invariants.checkArgument(keysOrRanges.domain() == Routable.Domain.Key, "Only key txn may do writes");
                        Seekables<?, ?> readKeysOrRanges = rs.nextBoolean() ? keysOrRanges : Keys.EMPTY;
                        ListRead read = new ListRead(Function.identity(), false, readKeysOrRanges, readKeysOrRanges);
                        ListQuery query = new ListQuery(null, 42, false);
                        ListUpdate update = new ListUpdate(Function.identity());
                        for (Key key : (Keys) keysOrRanges)
                            update.put(key, 1);
                        return new Txn.InMemory(kind, keysOrRanges, read, query, update);
                    }
                    case ExclusiveSyncPoint:
                    case SyncPoint:
                    {
                        return agent.emptyTxn(kind, keysOrRanges);
                    }
                    default:
                        throw new UnsupportedOperationException(kind.name());
                }
            };
        }
    }

    interface CommandTransformation
    {
        enum Status { Ok, Done}
        class Result
        {
            final Status status;
            @Nullable
            final Command command;

            public Result(Status status, @Nullable Command command)
            {
                this.status = status;
                this.command = command;
            }

            public static Result ok(@Nullable Command command)
            {
                return new Result(Status.Ok, command);
            }

            public static Result done(@Nullable Command command)
            {
                return new Result(Status.Done, command);
            }
        }
        Result transform(SafeCommandStore cs, SafeCommandsForKey cfk, @Nullable Command current);
    }

    private static class SimpleCommandTransformation implements CommandTransformation
    {
        private final String name;
        private final CommandTransformation delegate;

        private SimpleCommandTransformation(String name, CommandTransformation delegate)
        {
            this.name = name;
            this.delegate = delegate;
        }

        @Override
        public Result transform(SafeCommandStore cs, SafeCommandsForKey cfk, @Nullable Command current)
        {
            return delegate.transform(cs, cfk, current);
        }
    }

    private static class CommandUpdator
    {
        final TxnId txnId;
        final PeekingIterator<SimpleCommandTransformation> transformations;
        @Nullable
        Command current = null;

        private CommandUpdator(TxnId txnId, PeekingIterator<SimpleCommandTransformation> transformations)
        {
            this.txnId = txnId;
            this.transformations = transformations;
        }

        String name()
        {
            return transformations.peek().name;
        }
    }

    private static class SimpleCommand implements Property.Command<State, Void, Object>
    {
        private final String msg;
        private final Consumer<State> fn;

        private SimpleCommand(String msg, Consumer<State> fn)
        {
            this.msg = msg;
            this.fn = fn;
        }

        @Override
        public Object apply(State state)
        {
            fn.accept(state);
            return null;
        }

        @Override
        public Object run(Void sut)
        {
            return null;
        }

        @Override
        public String detailed(State state)
        {
            return msg;
        }
    }
}