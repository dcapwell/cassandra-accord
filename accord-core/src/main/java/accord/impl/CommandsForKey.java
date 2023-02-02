package accord.impl;

import accord.api.Key;
import accord.local.*;
import accord.primitives.*;
import com.google.common.collect.ImmutableSortedMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static accord.local.Status.KnownDeps.DepsUnknown;
import static accord.utils.Utils.*;

public class CommandsForKey extends ImmutableState
{
    // sentinel value to indicate a cfk requested in a preexecute context was not found
    // should not escape the safe command store
    public static final CommandsForKey EMPTY = new CommandsForKey(null, null)
    {
        @Override public Key key() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp max() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp lastExecutedTimestamp() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public long lastExecutedMicros() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp lastWriteTimestamp() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public CommandTimeseries<? extends TxnIdWithExecuteAt, ?> uncommitted() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public CommandTimeseries<TxnId, ?> committedById() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public CommandTimeseries<TxnId, ?> committedByExecuteAt() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }

        @Override
        public String toString()
        {
            return "CommandsForKey[EMPTY]";
        }
    };

    static
    {
        EMPTY.markInvalidated();
    }

    public static class SerializerSupport
    {
        public static CommandsForKey.Listener listener(Key key)
        {
            return new CommandsForKey.Listener(key);
        }

        public static  <D> CommandsForKey create(Key key, Timestamp max,
                                                 Timestamp lastExecutedTimestamp, long lastExecutedMicros, Timestamp lastWriteTimestamp,
                                                 CommandLoader<D> loader,
                                                 ImmutableSortedMap<Timestamp, D> uncommitted,
                                                 ImmutableSortedMap<Timestamp, D> committedById,
                                                 ImmutableSortedMap<Timestamp, D> committedByExecuteAt)
        {
            return new CommandsForKey(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp, loader, uncommitted, committedById, committedByExecuteAt);
        }
    }

    public interface CommandLoader<D>
    {
        D saveForCFK(Command command);

        TxnId txnId(D data);
        Timestamp executeAt(D data);
        Txn.Kind txnKind(D data);
        SaveStatus saveStatus(D data);
        PartialDeps partialDeps(D data);

        default Status status(D data)
        {
            return saveStatus(data).status;
        }

        default Status.Known known(D data)
        {
            return saveStatus(data).known;
        }
    }

    public static class CommandTimeseries<T, D>
    {
        public enum Kind { UNCOMMITTED, COMMITTED_BY_ID, COMMITTED_BY_EXECUTE_AT }
        /**
         * Test whether or not the dependencies of a command contain a given transaction id.
         * NOTE that this applies only to commands that have at least proposed dependencies;
         * if no dependencies are known the command will not be tested.
         */
        public enum TestDep { WITH, WITHOUT, ANY_DEPS }
        public enum TestStatus
        {
            IS, HAS_BEEN, ANY_STATUS;
            public static boolean test(Status test, TestStatus predicate, Status param)
            {
                return predicate == ANY_STATUS || (predicate == IS ? test == param : test.hasBeen(param));
            }
        }
        public enum TestKind { Ws, RorWs}

        private static <T, D> Stream<T> before(BiFunction<CommandLoader<D>, D, T> map, CommandLoader<D> loader, NavigableMap<Timestamp, D> commands, @Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return commands.headMap(timestamp, false).values().stream()
                    .filter(data -> testKind == RorWs || loader.txnKind(data) == WRITE)
                    .filter(data -> testDep == ANY_DEPS || (loader.known(data).deps != DepsUnknown && (loader.partialDeps(data).contains(depId) ^ (testDep == WITHOUT))))
                    .filter(data -> TestStatus.test(loader.status(data), testStatus, status))
                    .map(data -> map.apply(loader, data));
        }

        private static <T, D> Stream<T> after(BiFunction<CommandLoader<D>, D, T> map, CommandLoader<D> loader, NavigableMap<Timestamp, D> commands, @Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return commands.tailMap(timestamp, false).values().stream()
                    .filter(data -> testKind == RorWs || loader.txnKind(data) == WRITE)
                    // If we don't have any dependencies, we treat a dependency filter as a mismatch
                    .filter(data -> testDep == ANY_DEPS || (loader.known(data).deps != DepsUnknown && (loader.partialDeps(data).contains(depId) ^ (testDep == WITHOUT))))
                    .filter(data -> TestStatus.test(loader.status(data), testStatus, status))
                    .map(data -> map.apply(loader, data));
        }

        protected final BiFunction<CommandLoader<D>, D, T> map;
        protected final CommandLoader<D> loader;
        public final ImmutableSortedMap<Timestamp, D> commands;

        public CommandTimeseries(Update<T, D> builder)
        {
            this.map = builder.map;
            this.loader = builder.loader;
            this.commands = ensureSortedImmutable(builder.commands);
        }

        CommandTimeseries(BiFunction<CommandLoader<D>, D, T> map, CommandLoader<D> loader, ImmutableSortedMap<Timestamp, D> commands)
        {
            this.map = map;
            this.loader = loader;
            this.commands = commands;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CommandTimeseries<?, ?> that = (CommandTimeseries<?, ?>) o;
            return loader.equals(that.loader) && commands.equals(that.commands);
        }

        @Override
        public int hashCode()
        {
            int hash = 1;
            hash = 31 * hash + Objects.hashCode(loader);
            hash = 31 * hash + Objects.hashCode(commands);
            return hash;
        }

        public CommandTimeseries(BiFunction<CommandLoader<D>, D, T> map, CommandLoader<D> loader)
        {
            this(map, loader, ImmutableSortedMap.of());
        }

        public D get(Timestamp key)
        {
            return commands.get(key);
        }

        public boolean isEmpty()
        {
            return commands.isEmpty();
        }

        /**
         * All commands before (exclusive of) the given timestamp
         *
         * Note that {@code testDep} applies only to commands that know at least proposed deps; if specified any
         * commands that do not know any deps will be ignored.
         *
         * TODO (soon): TestDep should be asynchronous; data should not be kept memory-resident as only used for recovery
         *
         * TODO: we don't really need TestStatus anymore, but for clarity it might be nice to retain it to declare intent.
         *       This is because we only use it in places where TestDep is specified, and the statuses we want to rule-out
         *       do not have any deps.
         */
        public Stream<T> before(@Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return before(map, loader, commands, timestamp, testKind, testDep, depId, testStatus, status);
        }

        /**
         * All commands after (exclusive of) the given timestamp.
         *
         * Note that {@code testDep} applies only to commands that know at least proposed deps; if specified any
         * commands that do not know any deps will be ignored.
         */
        public Stream<T> after(@Nonnull Timestamp timestamp, @Nonnull TestKind testKind, @Nonnull TestDep testDep, @Nullable TxnId depId, @Nonnull TestStatus testStatus, @Nullable Status status)
        {
            return after(map, loader, commands, timestamp, testKind, testDep, depId, testStatus, status);
        }

        public Stream<T> between(Timestamp min, Timestamp max)
        {
            return commands.subMap(min, true, max, true).values().stream().map(data -> map.apply(loader, data));
        }

        public Stream<T> all()
        {
            return commands.values().stream().map(data -> map.apply(loader, data));
        }

        public static class Update<T, D>
        {
            protected BiFunction<CommandLoader<D>, D, T> map;
            protected CommandLoader<D> loader;
            protected NavigableMap<Timestamp, D> commands;

            public Update(BiFunction<CommandLoader<D>, D, T> map, CommandLoader<D> loader)
            {
                this.map = map;
                this.loader = loader;
                this.commands = new TreeMap<>();
            }

            public Update(CommandTimeseries<T, D> timeseries)
            {
                this.map = timeseries.map;
                this.loader = timeseries.loader;
                this.commands = timeseries.commands;
            }

            public void add(Timestamp timestamp, Command command)
            {
                commands = ensureSortedMutable(commands);
                commands.put(timestamp, loader.saveForCFK(command));
            }

            public void remove(Timestamp timestamp)
            {
                commands = ensureSortedMutable(commands);
                commands.remove(timestamp);
            }

            CommandTimeseries<T, D> build()
            {
                return new CommandTimeseries<>(this);
            }
        }
    }

    public static class Listener implements CommandListener
    {
        protected final Key listenerKey;

        private Listener(Key listenerKey)
        {
            this.listenerKey = listenerKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerKey.equals(that.listenerKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerKey);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerKey + '}';
        }

        public Key key()
        {
            return listenerKey;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, TxnId txnId)
        {
            CommandsForKeys.listenerUpdate(safeStore, safeStore.commandsForKey(listenerKey), safeStore.command(txnId));
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller, Keys.of(listenerKey));
        }
    }

    public static CommandListener listener(Key key)
    {
        return new Listener(key);
    }

    public static CommandListener listener(Seekable keyOrRange)
    {
        throw new UnsupportedOperationException("TODO");
    }

    public static CommandListener listener(Seekables<?, ?> keysOrRanges)
    {
        throw new UnsupportedOperationException("TODO");
    }

    public interface TxnIdWithExecuteAt
    {
        TxnId txnId();
        Timestamp executeAt();

        class Immutable implements TxnIdWithExecuteAt
        {
            private final TxnId txnId;
            private final Timestamp executeAt;

            public Immutable(TxnId txnId, Timestamp executeAt)
            {
                this.txnId = txnId;
                this.executeAt = executeAt;
            }

            @Override
            public TxnId txnId()
            {
                return txnId;
            }

            @Override
            public Timestamp executeAt()
            {
                return executeAt;
            }
        }

        static TxnIdWithExecuteAt from(Command command)
        {
            return new TxnIdWithExecuteAt.Immutable(command.txnId(), command.executeAt());
        }

        static <D> TxnIdWithExecuteAt from(CommandLoader<D> loader, D data)
        {
            return new TxnIdWithExecuteAt.Immutable(loader.txnId(data), loader.executeAt(data));
        }
    }

    // TODO (now): add validation that anything inserted into *committedBy* has everything prior in its dependencies
    private final Key key;
    private final Timestamp max;
    private final Timestamp lastExecutedTimestamp;
    private final long lastExecutedMicros;
    private final Timestamp lastWriteTimestamp;
    private final CommandTimeseries<TxnIdWithExecuteAt, ?> uncommitted;
    private final CommandTimeseries<TxnId, ?> committedById;
    private final CommandTimeseries<TxnId, ?> committedByExecuteAt;

    private  <D> CommandsForKey(Key key, Timestamp max,
                              Timestamp lastExecutedTimestamp, long lastExecutedMicros, Timestamp lastWriteTimestamp,
                              CommandLoader<D> loader,
                              ImmutableSortedMap<Timestamp, D> uncommitted,
                              ImmutableSortedMap<Timestamp, D> committedById,
                              ImmutableSortedMap<Timestamp, D> committedByExecuteAt)
    {
        this.key = key;
        this.max = max;
        this.lastExecutedTimestamp = lastExecutedTimestamp;
        this.lastExecutedMicros = lastExecutedMicros;
        this.lastWriteTimestamp = lastWriteTimestamp;
        this.uncommitted = new CommandTimeseries<>(TxnIdWithExecuteAt::from, loader, uncommitted);
        this.committedById = new CommandTimeseries<>(CommandLoader::txnId, loader, committedById);
        this.committedByExecuteAt = new CommandTimeseries<>(CommandLoader::txnId, loader, committedByExecuteAt);
    }

    public <D> CommandsForKey(Key key, CommandLoader<D> loader)
    {
        this.key = key;
        this.max = Timestamp.NONE;
        this.lastExecutedTimestamp = Timestamp.NONE;
        this.lastExecutedMicros = 0;
        this.lastWriteTimestamp = Timestamp.NONE;
        this.uncommitted = new CommandTimeseries<>(TxnIdWithExecuteAt::from, loader);
        this.committedById = new CommandTimeseries<>(CommandLoader::txnId, loader);
        this.committedByExecuteAt = new CommandTimeseries<>(CommandLoader::txnId, loader);
    }

    public CommandsForKey(Update builder)
    {
        this.key = builder.key;
        this.max = builder.max;
        this.lastExecutedTimestamp = builder.lastExecutedTimestamp;
        this.lastExecutedMicros = builder.lastExecutedMicros;
        this.lastWriteTimestamp = builder.lastWriteTimestamp;
        this.uncommitted = builder.uncommitted.build();
        this.committedById = builder.committedById.build();
        this.committedByExecuteAt = builder.committedByExecuteAt.build();
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return lastExecutedMicros == that.lastExecutedMicros
                && key.equals(that.key)
                && Objects.equals(max, that.max)
                && Objects.equals(lastExecutedTimestamp, that.lastExecutedTimestamp)
                && Objects.equals(lastWriteTimestamp, that.lastWriteTimestamp)
                && uncommitted.equals(that.uncommitted)
                && committedById.equals(that.committedById)
                && committedByExecuteAt.equals(that.committedByExecuteAt);
    }

    @Override
    public int hashCode()
    {
        int hash = 1;
        hash = 31 * hash + Objects.hashCode(key);
        hash = 31 * hash + Objects.hashCode(max);
        hash = 31 * hash + Objects.hashCode(lastExecutedTimestamp);
        hash = 31 * hash + Long.hashCode(lastExecutedMicros);
        hash = 31 * hash + Objects.hashCode(lastWriteTimestamp);
        hash = 31 * hash + uncommitted.hashCode();
        hash = 31 * hash + committedById.hashCode();
        hash = 31 * hash + committedByExecuteAt.hashCode();
        return hash;
    }

    public Key key()
    {
        checkCanReadFrom();
        return key;
    }

    public Timestamp max()
    {
        checkCanReadFrom();
        return max;
    }

    public Timestamp lastExecutedTimestamp()
    {
        return lastExecutedTimestamp;
    }

    public long lastExecutedMicros()
    {
        return lastExecutedMicros;
    }

    public Timestamp lastWriteTimestamp()
    {
        return lastWriteTimestamp;
    }

    public CommandTimeseries<? extends TxnIdWithExecuteAt, ?> uncommitted()
    {
        checkCanReadFrom();
        return uncommitted;
    }

    public CommandTimeseries<TxnId, ?> committedById()
    {
        checkCanReadFrom();
        return committedById;
    }

    public CommandTimeseries<TxnId, ?> committedByExecuteAt()
    {
        checkCanReadFrom();
        return committedByExecuteAt;
    }

    private static long getTimestampMicros(Timestamp timestamp)
    {
        return timestamp.real + timestamp.logical;
    }


    private void validateExecuteAtTime(Timestamp executeAt, boolean isForWriteTxn)
    {
        if (executeAt.compareTo(lastWriteTimestamp) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWriteTimestamp));

        int cmp = executeAt.compareTo(lastExecutedTimestamp);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecutedTimestamp));
        else
            throw new IllegalArgumentException(String.format("%s is greater than the most recent executed timestamp, cfk should be updated", executeAt, lastExecutedTimestamp));
    }

    public int nowInSecondsFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
        // we use the executeAt time instead of the monotonic database timestamp to prevent uneven
        // ttl expiration in extreme cases, ie 1M+ writes/second to a key causing timestamps to overflow
        // into the next second on some keys and not others.
        return Math.toIntExact(TimeUnit.MICROSECONDS.toSeconds(getTimestampMicros(lastExecutedTimestamp)));
    }

    public long timestampMicrosFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
        return lastExecutedMicros;
    }

    public static class Update
    {
        private final SafeCommandStore safeStore;
        private boolean completed = false;
        private final Key key;
        private final CommandsForKey original;
        private Timestamp max;
        private Timestamp lastExecutedTimestamp;
        private long lastExecutedMicros;
        private Timestamp lastWriteTimestamp;
        private final CommandTimeseries.Update<TxnIdWithExecuteAt, ?> uncommitted;
        private final CommandTimeseries.Update<TxnId, ?> committedById;
        private final CommandTimeseries.Update<TxnId, ?> committedByExecuteAt;

        protected  <T, D> CommandTimeseries.Update<T, D> seriesBuilder(BiFunction<CommandLoader<D>, D, T> map, CommandLoader<D> loader, CommandTimeseries.Kind kind)
        {
            return new CommandTimeseries.Update<>(map, loader);
        }

        protected  <T, D> CommandTimeseries.Update<T, D> seriesBuilder(CommandTimeseries<T, D> series, CommandTimeseries.Kind kind)
        {
            return new CommandTimeseries.Update<>(series);
        }

        public Update(SafeCommandStore safeStore, CommandsForKey original)
        {
            original.checkCanUpdate();
            this.safeStore = safeStore;
            this.original = original;
            this.key = original.key;
            this.max = original.max;
            this.lastExecutedTimestamp = original.lastExecutedTimestamp;
            this.lastExecutedMicros = original.lastExecutedMicros;
            this.lastWriteTimestamp = original.lastWriteTimestamp;
            this.uncommitted = seriesBuilder(original.uncommitted, CommandTimeseries.Kind.UNCOMMITTED);
            this.committedById = seriesBuilder(original.committedById, CommandTimeseries.Kind.COMMITTED_BY_ID);
            this.committedByExecuteAt = seriesBuilder(original.committedByExecuteAt, CommandTimeseries.Kind.COMMITTED_BY_EXECUTE_AT);
        }

        private void checkNotCompleted()
        {
            if (completed)
                throw new IllegalStateException(this + " has been completed");
        }

        public Key key()
        {
            return key;
        }

        public void updateMax(Timestamp timestamp)
        {
            checkNotCompleted();
            max = Timestamp.max(max, timestamp);
        }

        void addUncommitted(Command command)
        {
            checkNotCompleted();
            uncommitted.add(command.txnId(), command);
        }

        void removeUncommitted(Command command)
        {
            checkNotCompleted();
            uncommitted.remove(command.txnId());
        }

        void addCommitted(Command command)
        {
            checkNotCompleted();
            committedById.add(command.txnId(), command);
            committedByExecuteAt.add(command.executeAt(), command);
        }

        void updateLastExecutionTimestamps(Timestamp executeAt, boolean isForWriteTxn)
        {
            long micros = getTimestampMicros(executeAt);
            long lastMicros = lastExecutedMicros;

            lastExecutedTimestamp = executeAt;
            lastExecutedMicros = Math.max(micros, lastMicros + 1);
            if (isForWriteTxn)
                lastWriteTimestamp = executeAt;
        }

        public CommandsForKey complete()
        {
            checkNotCompleted();
            CommandsForKey updated = new CommandsForKey(this);
            if (original != null)
                original.markSuperseded();
            updated.markActive();
            safeStore.completeUpdate(this, original, updated);
            completed = true;
            return updated;
        }
    }

    public static CommandsForKey updateLastExecutionTimestamps(CommandsForKey current, SafeCommandStore safeStore, Timestamp executeAt, boolean isForWriteTxn)
    {
        Timestamp lastWrite = current.lastWriteTimestamp;

        if (executeAt.compareTo(lastWrite) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWrite));

        Timestamp lastExecuted = current.lastExecutedTimestamp;
        int cmp = executeAt.compareTo(lastExecuted);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return current;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecuted));

        Update update = safeStore.beginUpdate(current);
        update.updateLastExecutionTimestamps(executeAt, isForWriteTxn);
        return update.complete();
    }
}
