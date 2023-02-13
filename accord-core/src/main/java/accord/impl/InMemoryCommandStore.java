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

import accord.local.*;
import accord.local.SyncCommandStores.SyncCommandStore; // java8 fails compilation if this is in correct position
import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.CommandStores.RangesForEpoch;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.local.SafeCommandStore.TestDep.*;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.Committed;
import static accord.primitives.Routables.Slice.Minimal;

public interface InMemoryCommandStore extends CommandStore
{
    Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);

    SafeCommandStore beginOperation(PreLoadContext context);
    void completeOperation(SafeCommandStore store);

    static class RangeCommand
    {
        final LiveCommand command;
        Ranges ranges;

        RangeCommand(LiveCommand command)
        {
            this.command = command;
        }

        void update(Ranges add)
        {
            if (ranges == null) ranges = add;
            else ranges = ranges.with(add);
        }
    }

    class CFKLoader implements CommandsForKey.CommandLoader<TxnId>
    {
        private final State state;

        public CFKLoader(State state)
        {
            this.state = state;
        }

        private Command loadForCFK(TxnId data)
        {
            InMemorySafeStore safeStore = state.current;
            LiveCommand result;
            // simplifies tests
            if (safeStore != null)
            {
                result = safeStore.ifPresent(data);
                if (result != null)
                    return result.current();
            }
            result = state.command(data);
            if (result != null)
                return result.current();
            throw new IllegalStateException("Could not find command for CFK for " + data);
        }

        @Override
        public TxnId txnId(TxnId txnId)
        {
            return loadForCFK(txnId).txnId();
        }

        @Override
        public Timestamp executeAt(TxnId txnId)
        {
            return loadForCFK(txnId).executeAt();
        }

        @Override
        public SaveStatus saveStatus(TxnId txnId)
        {
            return loadForCFK(txnId).saveStatus();
        }

        @Override
        public PartialDeps partialDeps(TxnId txnId)
        {
            return loadForCFK(txnId).partialDeps();
        }

        @Override
        public TxnId saveForCFK(Command command)
        {
            return command.txnId();
        }
    }

    class State
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog progressLog;
        private final RangesForEpochHolder rangesForEpochHolder;

        private RangesForEpoch rangesForEpoch;
        private final CommandStore commandStore;
        private final NavigableMap<TxnId, InMemoryLiveCommand> commands = new TreeMap<>();
        private final NavigableMap<RoutableKey, InMemoryLiveCommandsForKey> commandsForKey = new TreeMap<>();
        private final CFKLoader cfkLoader;
        // TODO (find library, efficiency): this is obviously super inefficient, need some range map

        private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();

        private InMemorySafeStore current;

        public State(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLog = progressLog;
            this.rangesForEpochHolder = rangesForEpoch;
            this.commandStore = commandStore;
            this.cfkLoader = new CFKLoader(this);
        }

        public LiveCommand command(TxnId txnId)
        {
            return commands.computeIfAbsent(txnId, InMemoryLiveCommand::new);
        }

        public boolean hasCommand(TxnId txnId)
        {
            return commands.containsKey(txnId);
        }

        public LiveCommandsForKey commandsForKey(Key key)
        {
            return commandsForKey.computeIfAbsent(key, k -> new InMemoryLiveCommandsForKey((Key) k));
        }

        public boolean hasCommandsForKey(Key key)
        {
            return commandsForKey.containsKey(key);
        }

        void refreshRanges()
        {
            rangesForEpoch = rangesForEpochHolder.get();
        }

        public CFKLoader cfkLoader()
        {
            return cfkLoader;
        }

        public void forEpochCommands(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
            Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
            for (Range range : ranges)
            {
                Iterable<InMemoryLiveCommandsForKey> rangeCommands = commandsForKey.subMap(
                        range.start(), range.startInclusive(),
                        range.end(), range.endInclusive()
                ).values();

                for (InMemoryLiveCommandsForKey commands : rangeCommands)
                {
                    if (commands.isEmpty())
                        continue;
                    commands.current().forWitnessed(minTimestamp, maxTimestamp, txnId -> consumer.accept(command(txnId).current()));
                }
            }
        }

        public void forCommittedInEpoch(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
            Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
            for (Range range : ranges)
            {
                Iterable<InMemoryLiveCommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                                                                                       range.startInclusive(),
                                                                                       range.end(),
                                                                                       range.endInclusive()).values();
                for (InMemoryLiveCommandsForKey commands : rangeCommands)
                {
                    if (commands.isEmpty())
                        continue;
                    commands.current().byExecuteAt()
                            .between(minTimestamp, maxTimestamp, status -> status.hasBeen(Committed))
                            .forEach(txnId -> consumer.accept(command(txnId).current()));
                }
            }
        }

        public CommonAttributes register(InMemorySafeStore safeStore, Seekables<?, ?> keysOrRanges, Ranges slice, LiveCommand command, CommonAttributes attrs)
        {
            switch (keysOrRanges.domain())
            {
                default: throw new AssertionError();
                case Key:
                    CommonAttributes.Mutable mutable = attrs.mutableAttrs();
                    forEach(keysOrRanges, slice, key -> {
                        LiveCommandsForKey cfk = safeStore.commandsForKey(key);
                        CommandListener listener = CommandsForKey.listener(cfk.register(command.current()));
                        mutable.addListener(listener);
                    });
                    return mutable;
                case Range:
                    rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(command))
                            .update((Ranges)keysOrRanges);
            }
            return attrs;
        }

        public CommonAttributes register(InMemorySafeStore safeStore, Seekable keyOrRange, Ranges slice, LiveCommand command, CommonAttributes attrs)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    CommonAttributes.Mutable mutable = attrs.mutableAttrs();
                    forEach(keyOrRange, slice, key -> {
                        LiveCommandsForKey cfk = safeStore.commandsForKey(key);
                        CommandListener listener = CommandsForKey.listener(cfk.register(command.current()));
                        mutable.addListener(listener);
                    });
                    return mutable;
                case Range:
                    rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(command))
                            .update(Ranges.of((Range)keyOrRange));
            }
            return attrs;
        }

        private <O> O mapReduceForKey(InMemorySafeStore safeStore, Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<LiveCommandsForKey, O, O> map, O accumulate, O terminalValue)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    for (Key key : keys)
                    {
                        if (!slice.contains(key)) continue;
                        LiveCommandsForKey forKey = safeStore.ifLoaded(key);
                        accumulate = map.apply(forKey, accumulate);
                        if (accumulate.equals(terminalValue))
                            return accumulate;
                    }
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    Ranges sliced = ranges.slice(slice, Minimal);
                    for (Range range : sliced)
                    {
                        for (InMemoryLiveCommandsForKey forKey : commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).values())
                        {
                            accumulate = map.apply(forKey, accumulate);
                            if (accumulate.equals(terminalValue))
                                return accumulate;
                        }
                    }
            }
            return accumulate;
        }

        private void forEach(Seekables<?, ?> keysOrRanges, Ranges slice, Consumer<RoutableKey> forEach)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    keys.forEach(slice, key -> forEach.accept(key));
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    ranges.slice(slice).forEach(range -> {
                        commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                                .keySet().forEach(forEach);
                    });
            }
        }

        private void forEach(Routable keyOrRange, Ranges slice, Consumer<RoutableKey> forEach)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    Key key = (Key) keyOrRange;
                    if (slice.contains(key))
                        forEach.accept(key);
                    break;
                case Range:
                    Range range = (Range) keyOrRange;
                    Ranges.of(range).slice(slice).forEach(r -> {
                        commandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive())
                                .keySet().forEach(forEach);
                    });
            }
        }


        protected InMemorySafeStore createCommandStore(PreLoadContext context, Map<TxnId, LiveCommand> commands, Map<RoutableKey, LiveCommandsForKey> commandsForKeys)
        {
            return new InMemorySafeStore(this, cfkLoader, context, commands, commandsForKeys);
        }

        protected final InMemorySafeStore createCommandStore(PreLoadContext context)
        {
            Map<TxnId, LiveCommand> commands = new HashMap<>();
            Map<RoutableKey, LiveCommandsForKey> commandsForKeys = new HashMap<>();
            for (TxnId txnId : context.txnIds())
                commands.put(txnId, command(txnId));

            for (Seekable seekable : context.keys())
            {
                switch (seekable.domain())
                {
                    case Key:
                        RoutableKey key = (RoutableKey) seekable;
                        commandsForKeys.put(key, commandsForKey((Key) key));
                        break;
                    case Range:
                        // load range cfks here
                }
            }
            return createCommandStore(context, commands, commandsForKeys);
        }

        public SafeCommandStore beginOperation(PreLoadContext context)
        {
            refreshRanges();
            if (current != null)
                throw new IllegalStateException("Another operation is in progress or it's store was not cleared");
            current = createCommandStore(context);
            return current;
        }

        public void completeOperation(SafeCommandStore store)
        {
            if (store != current)
                throw new IllegalStateException("This operation has already been cleared");
            try
            {
                current.complete();
            }
            catch (Throwable t)
            {
                logger.error("Exception completing operation", t);
                throw t;
            }
            finally
            {
                current = null;
            }
        }

        private <T> T executeInContext(InMemoryCommandStore commandStore, PreLoadContext preLoadContext, Function<? super SafeCommandStore, T> function, boolean isDirectCall)
        {

            SafeCommandStore safeStore = commandStore.beginOperation(preLoadContext);
            try
            {
                return function.apply(safeStore);
            }
            catch (Throwable t)
            {
                if (isDirectCall) logger.error("Uncaught exception", t);
                throw t;
            }
            finally
            {
                commandStore.completeOperation(safeStore);
            }
        }

        protected <T> T executeInContext(InMemoryCommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return executeInContext(commandStore, context, function, true);

        }

        protected <T> void executeInContext(InMemoryCommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
        {
            try
            {
                T result = executeInContext(commandStore, context, function, false);
                callback.accept(result, null);
            }
            catch (Throwable t)
            {
                logger.error("Uncaught exception", t);
                callback.accept(null, t);
            }
        }

    }

    static <K, V> Function<K, V> getOrCreate(Function<K, V> get, Function<K, V> init)
    {
        return key -> {
            V value = get.apply(key);
            if (value != null)
                return value;
            return init.apply(key);
        };
    }

    class InMemorySafeStore extends AbstractSafeCommandStore<LiveCommand, LiveCommandsForKey>
    {
        private final State state;
        private final CFKLoader cfkLoader;

        public InMemorySafeStore(State state, CFKLoader cfkLoader, PreLoadContext context, Map<TxnId, LiveCommand> commands, Map<RoutableKey, LiveCommandsForKey> commandsForKey)
        {
            super(context, commands, commandsForKey);
            this.cfkLoader = cfkLoader;
            this.state = state;
        }

        @Override
        protected LiveCommand getIfLoaded(TxnId txnId)
        {
            return state.command(txnId);
        }

        @Override
        protected LiveCommandsForKey getIfLoaded(RoutableKey key)
        {
            return state.commandsForKey((Key) key);
        }

        @Override
        public CommandStore commandStore()
        {
            return state.commandStore;
        }

        @Override
        public DataStore dataStore()
        {
            return state.store;
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        @Override
        public ProgressLog progressLog()
        {
            return state.progressLog;
        }

        @Override
        public RangesForEpoch ranges()
        {
            Invariants.checkState(state.rangesForEpoch != null);
            return state.rangesForEpoch;
        }

        @Override
        public long latestEpoch()
        {
            return state.time.epoch();

        }

        @Override
        public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            Timestamp timestamp = state.mapReduceForKey(this, keysOrRanges, slice, (forKey, prev) -> Timestamp.max(forKey.current().max(), prev), Timestamp.NONE, null);
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            for (RangeCommand command : state.rangeCommands.values())
            {
                if (command.ranges.intersects(sliced))
                    timestamp = Timestamp.max(timestamp, command.command.current().executeAt());
            }
            return timestamp;
        }

        @Override
        public NodeTimeService time()
        {
            return state.time;
        }

        @Override
        public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
        {
            accumulate = state.mapReduceForKey(this, keysOrRanges, slice, (forKey, prev) -> {
                CommandsForKey.CommandTimeseries<?> timeseries;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case STARTED_BEFORE:
                        timeseries = forKey.current().byId();
                        break;
                    case EXECUTES_AFTER:
                    case MAY_EXECUTE_BEFORE:
                        timeseries = forKey.current().byExecuteAt();
                }
                CommandsForKey.CommandTimeseries.TestTimestamp remapTestTimestamp;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case EXECUTES_AFTER:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.AFTER;
                        break;
                    case STARTED_BEFORE:
                    case MAY_EXECUTE_BEFORE:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.BEFORE;
                }
                return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
            }, accumulate, terminalValue);

            if (accumulate.equals(terminalValue))
                return accumulate;

            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            Map<Range, List<Command>> collect = new TreeMap<>(Range::compare);
            state.rangeCommands.forEach(((txnId, rangeCommand) -> {
                Command command = rangeCommand.command.current();
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                        if (command.txnId().compareTo(timestamp) < 0) return;
                        else break;
                    case STARTED_BEFORE:
                        if (command.txnId().compareTo(timestamp) > 0) return;
                        else break;
                    case EXECUTES_AFTER:
                        if (command.executeAt().compareTo(timestamp) < 0) return;
                        else break;
                    case MAY_EXECUTE_BEFORE:
                        Timestamp compareTo = command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : command.txnId();
                        if (compareTo.compareTo(timestamp) > 0) return;
                        else break;
                }

                if (minStatus != null && command.status().compareTo(minStatus) < 0)
                    return;

                if (maxStatus != null && command.status().compareTo(maxStatus) > 0)
                    return;

                if (testKind == Ws && command.txnId().rw().isRead())
                    return;

                if (testDep != ANY_DEPS)
                {
                    if (!command.known().deps.hasProposedOrDecidedDeps())
                        return;

                    if ((testDep == WITH) == !command.partialDeps().contains(depId))
                        return;
                }

                if (!rangeCommand.ranges.intersects(sliced))
                    return;

                Routables.foldl(rangeCommand.ranges, sliced, (r, in, i) -> {
                    // TODO (easy, efficiency): pass command as a parameter to Fold
                    List<Command> list = in.computeIfAbsent(r, ignore -> new ArrayList<>());
                    if (list.isEmpty() || list.get(list.size() - 1) != command)
                        list.add(command);
                    return in;
                }, collect);
            }));
            for (Map.Entry<Range, List<Command>> e : collect.entrySet())
            {
                for (Command command : e.getValue())
                {
                    T initial = accumulate;
                    accumulate = map.apply(e.getKey(), command.txnId(), command.executeAt(), initial);
                }
            }

            return accumulate;
        }

        @Override
        public CommonAttributes completeRegistration(Seekables<?, ?> keysOrRanges, Ranges slice, LiveCommand command, CommonAttributes attrs)
        {
            return state.register(this, keysOrRanges, slice, command, attrs);
        }

        @Override
        public CommonAttributes completeRegistration(Seekable keyOrRange, Ranges slice, LiveCommand command, CommonAttributes attrs)
        {
            return state.register(this, keyOrRange, slice, command, attrs);
        }

        public CommandsForKey.CommandLoader<?> cfkLoader()
        {
            return cfkLoader;
        }
    }

    class Synchronized extends SyncCommandStore implements InMemoryCommandStore
    {
        Runnable active = null;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
        final State state;

        public Synchronized(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id);
            this.state = new State(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        private synchronized void maybeRun()
        {
            if (active != null)
                return;

            active = queue.poll();
            while (active != null)
            {
                try
                {
                    active.run();
                }
                catch (Throwable t)
                {
                    logger.error("Uncaught exception", t);
                }
                active = queue.poll();
            }
        }

        private void enqueueAndRun(Runnable runnable)
        {
            boolean result = queue.add(runnable);
            if (!result)
                throw new IllegalStateException("could not add item to queue");
            maybeRun();
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return submit(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                public void begin(BiConsumer<? super T, Throwable> callback)
                {
                    enqueueAndRun(() -> executeSync(context, function, callback));
                }
            };
        }

        private synchronized <T> void executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
        {
            enqueueAndRun(() -> state.executeInContext(this, context, function, callback));
        }

        @Override
        protected <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            AsyncResult.Settable<T> result = AsyncResults.settable();

            enqueueAndRun(() -> {
                try
                {
                    result.trySuccess(state.executeInContext(this, context, function));
                }
                catch (Throwable t)
                {
                    result.tryFailure(t);
                }
            });

            try
            {
                return AsyncResults.getBlocking(result);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e.getCause());
            }
        }

        @Override
        public SafeCommandStore beginOperation(PreLoadContext context)
        {
            return state.beginOperation(context);
        }

        @Override
        public void completeOperation(SafeCommandStore store)
        {
            state.completeOperation(store);
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread implements InMemoryCommandStore
    {
        private final int id;
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();
        private final ExecutorService executor;
        private final State state;

        public SingleThread(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            this.id = id;
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ']');
                return thread;
            });
            state = createState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }

        @Override
        public int id()
        {
            return 0;
        }

        protected State createState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
        {
            return new State(time, agent, store, progressLog, rangesForEpoch, this);
        }

        void assertThread()
        {
            Thread current = Thread.currentThread();
            Thread expected;
            while (true)
            {
                expected = expectedThread.get();
                if (expected != null)
                    break;
                expectedThread.compareAndSet(null, Thread.currentThread());
            }
            if (expected != current)
                throw new IllegalStateException(String.format("Command store called from the wrong thread. Expected %s, got %s", expected, current));
        }

        @Override
        public Agent agent()
        {
            return state.agent;
        }

        private State safeStore()
        {
            state.refreshRanges();
            return state;
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return submit(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return AsyncChains.ofCallable(executor, () -> state.executeInContext(this, context, function));
        }

        @Override
        public SafeCommandStore beginOperation(PreLoadContext context)
        {
            return state.beginOperation(context);
        }

        @Override
        public void completeOperation(SafeCommandStore store)
        {
            state.completeOperation(store);
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class Debug extends SingleThread
    {
        class DebugSafeStore extends InMemorySafeStore
        {
            public DebugSafeStore(State state, CFKLoader cfkLoader, PreLoadContext context, Map<TxnId, LiveCommand> commands, Map<RoutableKey, LiveCommandsForKey> commandsForKey)
            {
                super(state, cfkLoader, context, commands, commandsForKey);
            }

            @Override
            public LiveCommand ifPresent(TxnId txnId)
            {
                assertThread();
                return super.ifPresent(txnId);
            }

            @Override
            public LiveCommand ifLoaded(TxnId txnId)
            {
                assertThread();
                return super.ifLoaded(txnId);
            }

            @Override
            public LiveCommand command(TxnId txnId)
            {
                assertThread();
                return super.command(txnId);
            }

            @Override
            public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
            {
                assertThread();
                super.register(keysOrRanges, slice, command);
            }

            @Override
            public void register(Seekable keyOrRange, Ranges slice, Command command)
            {
                assertThread();
                super.register(keyOrRange, slice, command);
            }
        }

        class DebugState extends State
        {
            public DebugState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
            {
                super(time, agent, store, progressLog, rangesForEpoch, commandStore);
            }

            @Override
            protected InMemorySafeStore createCommandStore(PreLoadContext context, Map<TxnId, LiveCommand> commands, Map<RoutableKey, LiveCommandsForKey> commandsForKeys)
            {
                return new DebugSafeStore(this, cfkLoader(), context, commands, commandsForKeys);
            }
        }


        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();

        public Debug(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        protected State createState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
        {
            return new DebugState(time, agent, store, progressLog, rangesForEpoch, commandStore);
        }
    }

    public static State inMemory(CommandStore unsafeStore)
    {
        return (unsafeStore instanceof Synchronized) ? ((Synchronized) unsafeStore).state : ((SingleThread) unsafeStore).state;
    }

    public static State inMemory(SafeCommandStore safeStore)
    {
        return inMemory(safeStore.commandStore());
    }
}
