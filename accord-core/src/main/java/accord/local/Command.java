package accord.local;

import accord.api.Data;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeys;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.Utils;
import accord.utils.async.AsyncChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import javax.annotation.Nullable;
import java.util.*;

import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Known.DefinitionOnly;
import static accord.utils.Utils.*;
import static java.lang.String.format;

public abstract class Command extends ImmutableState
{
    // sentinel value to indicate a command requested in a preexecute context was not found
    // should not escape the safe command store
    public static final Command EMPTY = new Command()
    {
        @Override public Route<?> route() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public RoutingKey progressKey() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public RoutingKey homeKey() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public TxnId txnId() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Ballot promised() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Status.Durability durability() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public ImmutableSet<CommandListener> listeners() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public SaveStatus saveStatus() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Timestamp executeAt() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public Ballot accepted() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Override public PartialTxn partialTxn() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }
        @Nullable
        @Override public PartialDeps partialDeps() { throw new IllegalStateException("Attempting to access EMPTY sentinel values"); }

        @Override
        public String toString()
        {
            return "Command(EMPTY)";
        }
    };

    static
    {
        EMPTY.markInvalidated();
    }

    static PreLoadContext contextForCommand(Command command)
    {
        Invariants.checkState(command.hasBeen(Status.PreAccepted) && command.partialTxn() != null);
        return command instanceof PreLoadContext ? (PreLoadContext) command : PreLoadContext.contextFor(command.txnId(), command.partialTxn().keys());
    }

    private static Status.Durability durability(Status.Durability durability, SaveStatus status)
    {
        if (status.compareTo(SaveStatus.PreApplied) >= 0 && durability == NotDurable)
            return Local; // not necessary anywhere, but helps for logical consistency
        return durability;
    }

    public interface CommonAttributes
    {
        TxnId txnId();
        Status.Durability durability();
        RoutingKey homeKey();
        RoutingKey progressKey();
        Route<?> route();
        PartialTxn partialTxn();
        PartialDeps partialDeps();
        ImmutableSet<CommandListener> listeners();
    }

    public static class SerializerSupport
    {
        public static Command.Listener listener(TxnId txnId)
        {
            return new Command.Listener(txnId);
        }

        public static NotWitnessed notWitnessed(CommonAttributes attributes, Ballot promised)
        {
            return NotWitnessed.Factory.create(attributes, promised);
        }

        public static Preaccepted preaccepted(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return Preaccepted.Factory.create(common, executeAt, promised);
        }

        public static Accepted accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return Accepted.Factory.create(common, status, executeAt, promised, accepted);
        }

        public static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            return Committed.Factory.create(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            return Executed.Factory.create(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
        }
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> expected, Class<?> actual)
    {
        if (actual != expected)
        {
            throw new IllegalStateException(format("Cannot instantiate %s for status %s. %s expected",
                                                   actual.getSimpleName(), status, expected.getSimpleName()));
        }
        return status;
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> klass)
    {
        switch (status)
        {
            case NotWitnessed:
                return validateCommandClass(status, NotWitnessed.class, klass);
            case PreAccepted:
                return validateCommandClass(status, Preaccepted.class, klass);
            case AcceptedInvalidate:
            case AcceptedInvalidateWithDefinition:
            case Accepted:
            case AcceptedWithDefinition:
                return validateCommandClass(status, Accepted.class, klass);
            case Committed:
            case ReadyToExecute:
                return validateCommandClass(status, Committed.class, klass);
            case PreApplied:
            case Applied:
            case Invalidated:
                return validateCommandClass(status, Executed.class, klass);
            default:
                throw new IllegalStateException("Unhandled status " + status);
        }
    }

    public static Command addListener(SafeCommandStore safeStore, Command command, CommandListener listener)
    {
        return safeStore.beginUpdate(command).addListener(listener).updateAttributes();
    }

    public static Command removeListener(SafeCommandStore safeStore, Command command, CommandListener listener)
    {
        return safeStore.beginUpdate(command).removeListener(listener).updateAttributes();
    }

    public static Committed updateWaitingOn(SafeCommandStore safeStore, Committed command, WaitingOn.Update waitingOn)
    {
        if (!waitingOn.hasChanges())
            return command;

        Update update = safeStore.beginUpdate(command);
        Committed updated =  command instanceof Executed ?
                Executed.Factory.update(command.asExecuted(), update, waitingOn.build()) :
                Committed.Factory.update(command, update, waitingOn.build());
        return update.complete(updated);
    }

    public static class Listener implements CommandListener
    {
        protected final TxnId listenerId;

        private Listener(TxnId listenerId)
        {
            this.listenerId = listenerId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerId.equals(that.listenerId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerId);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerId + '}';
        }

        public TxnId txnId()
        {
            return listenerId;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, TxnId txnId)
        {
            Commands.listenerUpdate(safeStore, safeStore.command(listenerId), safeStore.command(txnId));
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(Utils.listOf(listenerId, caller), Keys.EMPTY);
        }
    }

    public static CommandListener listener(TxnId txnId)
    {
        return new Listener(txnId);
    }

    private abstract static class AbstractCommand extends Command
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final Status.Durability durability;
        private final RoutingKey homeKey;
        private final RoutingKey progressKey;
        private final Route<?> route;
        private final Ballot promised;
        private final ImmutableSet<CommandListener> listeners;

        private AbstractCommand(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Route<?> route, Ballot promised, ImmutableSet<CommandListener> listeners)
        {
            this.txnId = txnId;
            this.status = validateCommandClass(status, getClass());
            this.durability = durability;
            this.homeKey = homeKey;
            this.progressKey = progressKey;
            this.route = route;
            this.promised = promised;
            this.listeners = listeners;
        }

        private AbstractCommand(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            this.txnId = common.txnId();
            this.status = validateCommandClass(status, getClass());
            this.durability = common.durability();
            this.homeKey = common.homeKey();
            this.progressKey = common.progressKey();
            this.route = common.route();
            this.promised = promised;
            this.listeners = common.listeners();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Command command = (Command) o;
            return txnId.equals(command.txnId())
                    && status == command.saveStatus()
                    && durability == command.durability()
                    && Objects.equals(homeKey, command.homeKey())
                    && Objects.equals(progressKey, command.progressKey())
                    && Objects.equals(route, command.route())
                    && Objects.equals(promised, command.promised())
                    && listeners.equals(command.listeners());
        }

        @Override
        public String toString()
        {
            return "Command@" + System.identityHashCode(this) + '{' + txnId + ':' + status + '}';
        }

        @Override
        public int hashCode()
        {
            int hash = 1;
            hash = 31 * hash + txnId.hashCode();
            hash = 31 * hash + status.hashCode();
            hash = 31 * hash + Objects.hashCode(durability);
            hash = 31 * hash + Objects.hashCode(homeKey);
            hash = 31 * hash + Objects.hashCode(progressKey);
            hash = 31 * hash + Objects.hashCode(route);
            hash = 31 * hash + Objects.hashCode(promised);
            hash = 31 * hash + Objects.hashCode(listeners);
            return hash;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public final RoutingKey homeKey()
        {
            checkCanReadFrom();
            return homeKey;
        }

        @Override
        public final RoutingKey progressKey()
        {
            checkCanReadFrom();
            return progressKey;
        }

        @Override
        public final Route<?> route()
        {
            checkCanReadFrom();
            return route;
        }

        @Override
        public Ballot promised()
        {
            checkCanReadFrom();
            return promised;
        }

        @Override
        public Status.Durability durability()
        {
            checkCanReadFrom();
            return Command.durability(durability, saveStatus());
        }

        @Override
        public ImmutableSet<CommandListener> listeners()
        {
            checkCanReadFrom();
            if (listeners == null)
                return ImmutableSet.of();
            return listeners;
        }

        @Override
        public final SaveStatus saveStatus()
        {
            checkCanReadFrom();
            return status;
        }
    }

    /**
     * If this is the home shard, we require that this is a Route for all states &gt; NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     */
    public abstract Route<?> route();
    public abstract RoutingKey progressKey();

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     *
     * TODO (expected, efficiency): we probably do not want to save this on its own, as we probably want to
     *  minimize IO interactions and discrete registers, so will likely reference commit log entries directly
     *  At which point we may impose a requirement that only a Route can be saved, not a homeKey on its own.
     *  Once this restriction is imposed, we no longer need to pass around Routable.Domain with TxnId.
     */
    public abstract RoutingKey homeKey();
    public abstract TxnId txnId();
    public abstract Ballot promised();
    public abstract Status.Durability durability();
    public abstract ImmutableSet<CommandListener> listeners();
    public abstract SaveStatus saveStatus();

    private static boolean isSameClass(Command command, Class<? extends Command> klass)
    {
        return command.getClass() == klass;
    }

    private static void checkNewBallot(Ballot current, Ballot next, String name)
    {
        if (next.compareTo(current) < 0)
            throw new IllegalArgumentException(String.format("Cannot update %s ballot from %s to %s. New ballot is less than current", name, current, next));
    }

    private static void checkPromised(Command command, Ballot ballot)
    {
        checkNewBallot(command.promised(), ballot, "promised");
    }

    private static void checkAccepted(Command command, Ballot ballot)
    {
        checkNewBallot(command.accepted(), ballot, "accepted");
    }

    private static void checkSameClass(Command command, Class<? extends Command> klass, String errorMsg)
    {
        if (!isSameClass(command, klass))
            throw new IllegalArgumentException(errorMsg + format(" expected %s got %s", klass.getSimpleName(), command.getClass().getSimpleName()));
    }

    // TODO (low priority, progress): callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public final Route<?> someRoute()
    {
        checkCanReadFrom();
        if (route() != null)
            return route();

        if (homeKey() != null)
            return PartialRoute.empty(txnId().domain(), homeKey());

        return null;
    }

    public Unseekables<?, ?> maxUnseekables()
    {
        Route<?> route = someRoute();
        if (route == null)
            return null;

        return route.toMaximalUnseekables();
    }

    public PreLoadContext contextForSelf()
    {
        checkCanReadFrom();
        return contextForCommand(this);
    }

    public abstract Timestamp executeAt();
    public abstract Ballot accepted();
    public abstract PartialTxn partialTxn();
    public abstract @Nullable PartialDeps partialDeps();

    public final Status status()
    {
        checkCanReadFrom();
        return saveStatus().status;
    }

    public final Status.Known known()
    {
        checkCanReadFrom();
        return saveStatus().known;
    }

    public boolean hasBeenWitnessed()
    {
        checkCanReadFrom();
        return partialTxn() != null;
    }

    public final boolean hasBeen(Status status)
    {
        return status().compareTo(status) >= 0;
    }

    public boolean has(Status.Known known)
    {
        return known.isSatisfiedBy(saveStatus().known);
    }

    public boolean has(Status.Definition definition)
    {
        return known().definition.compareTo(definition) >= 0;
    }

    public boolean has(Status.Outcome outcome)
    {
        return known().outcome.compareTo(outcome) >= 0;
    }

    public boolean is(Status status)
    {
        return status() == status;
    }

    public final CommandListener asListener()
    {
        return listener(txnId());
    }

    public final boolean isWitnessed()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.PreAccepted);
        Invariants.checkState(result == (this instanceof Preaccepted));
        return result;
    }

    public final Preaccepted asWitnessed()
    {
        checkCanReadFrom();
        return (Preaccepted) this;
    }

    public final boolean isAccepted()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.AcceptedInvalidate);
        Invariants.checkState(result == (this instanceof Accepted));
        return result;
    }

    public final Accepted asAccepted()
    {
        checkCanReadFrom();
        return (Accepted) this;
    }

    public final boolean isCommitted()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.Committed);
        Invariants.checkState(result == (this instanceof Committed));
        return result;
    }

    public final Committed asCommitted()
    {
        checkCanReadFrom();
        return (Committed) this;
    }

    public final boolean isExecuted()
    {
        checkCanReadFrom();
        boolean result = status().hasBeen(Status.PreApplied);
        Invariants.checkState(result == (this instanceof Executed));
        return result;
    }

    public final Executed asExecuted()
    {
        checkCanReadFrom();
        return (Executed) this;
    }

    public static final class NotWitnessed extends AbstractCommand
    {
        NotWitnessed(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Route<?> route, Ballot promised, ImmutableSet<CommandListener> listeners)
        {
            super(txnId, status, durability, homeKey, progressKey, route, promised, listeners);
        }

        NotWitnessed(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            super(common, status, promised);
        }

        public static NotWitnessed create(TxnId txnId)
        {
            return new NotWitnessed(txnId, SaveStatus.NotWitnessed, NotDurable, null, null, null, Ballot.ZERO, null);
        }

        private static class Factory
        {
            public static NotWitnessed create(CommonAttributes common, Ballot promised)
            {
                return new NotWitnessed(common, SaveStatus.NotWitnessed, promised);
            }

            public static NotWitnessed update(NotWitnessed command, CommonAttributes common, Ballot promised)
            {
                checkSameClass(command, NotWitnessed.class, "Cannot update");
                command.checkCanReadFrom();
                Invariants.checkArgument(command.txnId().equals(common.txnId()));
                return new NotWitnessed(common, command.saveStatus(), promised);
            }
        }

        @Override
        public Timestamp executeAt()
        {
            checkCanReadFrom();
            return null;
        }

        @Override
        public Ballot promised()
        {
            checkCanReadFrom();
            return Ballot.ZERO;
        }

        @Override
        public Ballot accepted()
        {
            checkCanReadFrom();
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkCanReadFrom();
            return null;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            checkCanReadFrom();
            return null;
        }
    }

    public static class Preaccepted extends AbstractCommand
    {
        private final Timestamp executeAt;
        private final PartialTxn partialTxn;
        private final @Nullable PartialDeps partialDeps;

        private Preaccepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised)
        {
            super(common, status, promised);
            this.executeAt = executeAt;
            this.partialTxn = common.partialTxn();
            this.partialDeps = common.partialDeps();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Preaccepted that = (Preaccepted) o;
            return executeAt.equals(that.executeAt)
                    && Objects.equals(partialTxn, that.partialTxn)
                    && Objects.equals(partialDeps, that.partialDeps);
        }

        @Override
        public int hashCode()
        {
            int hash = super.hashCode();
            hash = 31 * hash + Objects.hashCode(executeAt);
            hash = 31 * hash + Objects.hashCode(partialTxn);
            hash = 31 * hash + Objects.hashCode(partialDeps);
            return hash;
        }

        private static class Factory
        {
            public static Preaccepted create(CommonAttributes common, Timestamp executeAt, Ballot promised)
            {
                return new Preaccepted(common, SaveStatus.PreAccepted, executeAt, promised);
            }

            public static Preaccepted update(Preaccepted command, CommonAttributes common, Ballot promised)
            {
                checkPromised(command, promised);
                checkSameClass(command, Preaccepted.class, "Cannot update");
                Invariants.checkArgument(command.getClass() == Preaccepted.class);
                command.checkCanReadFrom();
                return create(common, command.executeAt(), promised);
            }
        }

        @Override
        public Timestamp executeAt()
        {
            checkCanReadFrom();
            return executeAt;
        }

        @Override
        public Ballot accepted()
        {
            checkCanReadFrom();
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkCanReadFrom();
            return partialTxn;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            checkCanReadFrom();
            return partialDeps;
        }
    }

    public static class Accepted extends Preaccepted
    {
        private final Ballot accepted;

        private Accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            super(common, status, executeAt, promised);
            this.accepted = accepted;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Accepted that = (Accepted) o;
            return Objects.equals(accepted, that.accepted);
        }

        @Override
        public int hashCode()
        {
            int hash = super.hashCode();
            hash = 31 * hash + Objects.hashCode(accepted);
            return hash;
        }

        private static class Factory
        {
            static Accepted create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
            {
                return new Accepted(common, status, executeAt, promised, accepted);
            }

            static Accepted update(Accepted command, CommonAttributes common, SaveStatus status, Ballot promised)
            {
                checkPromised(command, promised);
                checkSameClass(command, Accepted.class, "Cannot update");
                command.checkCanUpdate();
                return new Accepted(common, status, command.executeAt(), promised, command.accepted());
            }

            static Accepted update(Accepted command, CommonAttributes common, Ballot promised)
            {
                return update(command, common, command.saveStatus(), promised);
            }
        }

        @Override
        public Ballot accepted()
        {
            checkCanReadFrom();
            return accepted;
        }
    }

    public static class Committed extends Accepted
    {
        private final ImmutableSortedSet<TxnId> waitingOnCommit;
        private final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            super(common, status, executeAt, promised, accepted);
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            this(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Committed committed = (Committed) o;
            return Objects.equals(waitingOnCommit, committed.waitingOnCommit)
                    && Objects.equals(waitingOnApply, committed.waitingOnApply);
        }

        @Override
        public int hashCode()
        {
            int hash = super.hashCode();
            hash = 31 * hash + Objects.hashCode(waitingOnCommit);
            hash = 31 * hash + Objects.hashCode(waitingOnApply);
            return hash;
        }

        private static class Factory
        {
            private static Committed update(Committed command, CommonAttributes common, Ballot promised, SaveStatus status, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
            {
                checkPromised(command, promised);
                checkSameClass(command, Committed.class, "Cannot update");
                return new Committed(common, status, command.executeAt(), promised, command.accepted(), waitingOnCommit, waitingOnApply);
            }

            private static Committed update(Committed command, CommonAttributes common, Ballot promised)
            {
                return update(command, common, promised, command.saveStatus(), command.waitingOnCommit(), command.waitingOnApply());
            }

            private static Committed update(Committed command, CommonAttributes common, SaveStatus status)
            {
                return update(command, common, command.promised(), status, command.waitingOnCommit(), command.waitingOnApply());
            }

            private static Committed update(Committed command, CommonAttributes common, WaitingOn waitingOn)
            {
                return update(command, common, command.promised(), command.saveStatus(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
            }

            public static Committed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
            {
                return new Committed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
            }

            public static Committed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
            {
                return new Committed(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
            }
        }

        public AsyncChain<Data> read(SafeCommandStore safeStore)
        {
            checkCanReadFrom();
            return partialTxn().read(safeStore, this);
        }

        public WaitingOn waitingOn()
        {
            return new WaitingOn(waitingOnCommit, waitingOnApply);
        }

        public ImmutableSortedSet<TxnId> waitingOnCommit()
        {
            checkCanReadFrom();
            return waitingOnCommit;
        }

        public boolean isWaitingOnCommit()
        {
            checkCanReadFrom();
            return waitingOnCommit != null && !waitingOnCommit.isEmpty();
        }

        public TxnId firstWaitingOnCommit()
        {
            checkCanReadFrom();
            return isWaitingOnCommit() ? waitingOnCommit.first() : null;
        }

        public ImmutableSortedMap<Timestamp, TxnId> waitingOnApply()
        {
            checkCanReadFrom();
            return waitingOnApply;
        }

        public boolean isWaitingOnApply()
        {
            checkCanReadFrom();
            return waitingOnApply != null && !waitingOnApply.isEmpty();
        }

        public TxnId firstWaitingOnApply()
        {
            checkCanReadFrom();
            return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
        }

        public boolean hasBeenWitnessed()
        {
            checkCanReadFrom();
            return partialTxn() != null;
        }

        public boolean isWaitingOnDependency()
        {
            checkCanReadFrom();
            return isWaitingOnCommit() || isWaitingOnApply();
        }
    }

    public static class Executed extends Committed
    {
        private final Writes writes;
        private final Result result;

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
            this.writes = writes;
            this.result = result;
        }

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOn);
            this.writes = writes;
            this.result = result;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Executed executed = (Executed) o;
            return Objects.equals(writes, executed.writes)
                    && Objects.equals(result, executed.result);
        }

        @Override
        public int hashCode()
        {
            // AILA, Mum
            int hash = super.hashCode();
            hash = 31 * hash + Objects.hashCode(writes);
            hash = 31 * hash + Objects.hashCode(result);
            return hash;
        }

        private static class Factory
        {
            public static Executed update(Executed command, CommonAttributes common, SaveStatus status, Ballot promised, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
            {
                checkSameClass(command, Executed.class, "Cannot update");
                command.checkCanUpdate();
                return new Executed(common, status, command.executeAt(), promised, command.accepted(), waitingOnCommit, waitingOnApply, command.writes(), command.result());
            }

            public static Executed update(Executed command, CommonAttributes common, SaveStatus status)
            {
                return update(command, common, status, command.promised(), command.waitingOnCommit(), command.waitingOnApply());
            }

            public static Executed update(Executed command, CommonAttributes common, WaitingOn waitingOn)
            {
                return update(command, common, command.saveStatus(), command.promised(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
            }

            public static Executed update(Executed command, CommonAttributes common, Ballot promised)
            {
                return update(command, common, command.saveStatus(), promised, command.waitingOnCommit(), command.waitingOnApply());
            }

            public static Executed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
            {
                return new Executed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
            }

            public static Executed create(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
            {
                return new Executed(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply, writes, result);
            }
        }

        public Writes writes()
        {
            checkCanReadFrom();
            return writes;
        }

        public Result result()
        {
            checkCanReadFrom();
            return result;
        }
    }

    public static class WaitingOn
    {
        public static final WaitingOn EMPTY = new WaitingOn(ImmutableSortedSet.of(), ImmutableSortedMap.of());
        public final ImmutableSortedSet<TxnId> waitingOnCommit;
        public final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        public WaitingOn(ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        public static class Update
        {
            private boolean hasChanges = false;
            private NavigableSet<TxnId> waitingOnCommit;
            private NavigableMap<Timestamp, TxnId> waitingOnApply;

            public Update()
            {

            }

            public Update(WaitingOn waitingOn)
            {
                this.waitingOnCommit = waitingOn.waitingOnCommit;
                this.waitingOnApply = waitingOn.waitingOnApply;
            }

            public Update(Committed committed)
            {
                this.waitingOnCommit = committed.waitingOnCommit();
                this.waitingOnApply = committed.waitingOnApply();
            }

            public boolean hasChanges()
            {
                return hasChanges;
            }

            public void addWaitingOnCommit(TxnId txnId)
            {
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.add(txnId);
                hasChanges = true;
            }

            public void removeWaitingOnCommit(TxnId txnId)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.remove(txnId);
                hasChanges = true;
            }

            public void addWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.put(executeAt, txnId);
                hasChanges = true;
            }

            public void removeWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.remove(executeAt);
                hasChanges = true;
            }

            public void removeWaitingOn(TxnId txnId, Timestamp executeAt)
            {
                removeWaitingOnCommit(txnId);
                removeWaitingOnApply(txnId, executeAt);
                hasChanges = true;
            }

            public WaitingOn build()
            {
                if ((waitingOnCommit == null || waitingOnCommit.isEmpty()) && (waitingOnApply == null || waitingOnApply.isEmpty()))
                    return EMPTY;
                return new WaitingOn(ensureSortedImmutable(waitingOnCommit), ensureSortedImmutable(waitingOnApply));
            }
        }
    }

    private static Command updateAttributes(Command command, CommonAttributes attributes, Ballot promised)
    {
        switch (command.saveStatus())
        {
            case NotWitnessed:
                return NotWitnessed.Factory.update((NotWitnessed) command, attributes, promised);
            case PreAccepted:
                return Preaccepted.Factory.update((Preaccepted) command, attributes, promised);
            case AcceptedInvalidate:
            case AcceptedInvalidateWithDefinition:
            case Accepted:
            case AcceptedWithDefinition:
                return Accepted.Factory.update((Accepted) command, attributes, promised);
            case Committed:
            case ReadyToExecute:
                return Committed.Factory.update((Committed) command, attributes, promised);
            case PreApplied:
            case Applied:
            case Invalidated:
                return Executed.Factory.update((Executed) command, attributes, promised);
            default:
                throw new IllegalStateException("Unhandled status " + command.status());
        }
    }

    private static Command updateAttributes(Command command, CommonAttributes attributes)
    {
        return updateAttributes(command, attributes, command.promised());
    }

    public static class Update implements CommonAttributes
    {
        private static class ToRegister<T>
        {
            private final T value;
            private final Ranges slice;

            public ToRegister(T value, Ranges slice)
            {
                this.value = value;
                this.slice = slice;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ToRegister<?> that = (ToRegister<?>) o;
                return value.equals(that.value) && slice.equals(that.slice);
            }

            @Override
            public int hashCode()
            {
                int hash = 1;
                hash = 31 * hash + value.hashCode();
                hash = 31 * hash + slice.hashCode();
                return hash;
            }
        }

        private boolean completed = false;
        public final SafeCommandStore safeStore;
        private final Command command;

        private RoutingKey homeKey;
        private RoutingKey progressKey;
        private Route<?> route;
        private Status.Durability durability;

        private PartialTxn partialTxn;
        private @Nullable PartialDeps partialDeps;

        private Set<ToRegister<Seekable>> singleRegistrations = new HashSet<>();
        private Set<ToRegister<Seekables<?, ?>>> multiRegistrations = new HashSet<>();
        private Set<CommandListener> listeners;

        public Update(SafeCommandStore safeStore, Command command)
        {
            this.safeStore = safeStore;
            command.checkCanUpdate();
            this.command = command;
            this.homeKey = command.homeKey();
            this.progressKey = command.progressKey();
            this.route = command.route();
            this.durability = command.durability();
            this.listeners = command.listeners();
            if (command.isWitnessed())
            {
                Preaccepted preaccepted = command.asWitnessed();
                this.partialTxn = preaccepted.partialTxn();
                this.partialDeps = preaccepted.partialDeps();
            }
        }

        private void checkNotCompleted()
        {
            if (completed)
                throw new IllegalStateException(this + " has been completed");
        }

        @Override
        public TxnId txnId()
        {
            return command.txnId();
        }

        public Status status()
        {
            return command.status();
        }

        @Override
        public RoutingKey homeKey()
        {
            checkNotCompleted();
            return homeKey;
        }

        public Update homeKey(RoutingKey homeKey)
        {
            checkNotCompleted();
            this.homeKey = homeKey;
            return this;
        }

        @Override
        public RoutingKey progressKey()
        {
            checkNotCompleted();
            return progressKey;
        }

        /**
         * A key nominated to be the primary shard within this node for managing progress of the command.
         * It is nominated only as of txnId.epoch, and may be null (indicating that this node does not monitor
         * the progress of this command).
         *
         * Preferentially, this is homeKey on nodes that replicate it, and otherwise any key that is replicated, as of txnId.epoch
         */
        public Update progressKey(RoutingKey progressKey)
        {
            checkNotCompleted();
            RoutingKey current = command.progressKey();
            Invariants.checkArgument(current == null || current.equals(progressKey));
            this.progressKey = progressKey;
            return this;
        }

        @Override
        public Route<?> route()
        {
            checkNotCompleted();
            return route;
        }

        public Update route(Route<?> route)
        {
            checkNotCompleted();
            this.route = route;
            return this;
        }

        @Override
        public Status.Durability durability()
        {
            checkNotCompleted();
            return Command.durability(durability, command.saveStatus());
        }

        public Update durability(Status.Durability durability)
        {
            checkNotCompleted();
            this.durability = durability;
            return this;
        }

        @Override
        public ImmutableSet<CommandListener> listeners()
        {
            return ensureImmutable(listeners);
        }

        public Update addListener(CommandListener listener)
        {
            listeners = ensureMutable(listeners);
            listeners.add(listener);
            return this;
        }

        public Update removeListener(CommandListener listener)
        {
            if (listener == null || listeners.isEmpty())
                return this;
            listeners = ensureMutable(listeners);
            listeners.remove(listener);
            return this;
        }

        @Override
        public PartialTxn partialTxn()
        {
            checkNotCompleted();
            return partialTxn;
        }

        public Update partialTxn(PartialTxn partialTxn)
        {
            checkNotCompleted();
            this.partialTxn = partialTxn;
            return this;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            checkNotCompleted();
            return partialDeps;
        }

        public Update partialDeps(@Nullable PartialDeps partialDeps)
        {
            checkNotCompleted();
            this.partialDeps = partialDeps;
            return this;
        }

        public Update registerWith(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            checkNotCompleted();
            multiRegistrations.add(new ToRegister<>(keysOrRanges, slice));
            return this;
        }


        public Update registerWith(Seekable keyOrRange, Ranges slice)
        {
            checkNotCompleted();
            singleRegistrations.add(new ToRegister<>(keyOrRange, slice));
            return this;
        }

        protected  <T extends Command> T complete(T updated)
        {
            checkNotCompleted();

            if (updated == command)
                throw new IllegalStateException("Update is the same as the original");

            updated.markActive();

            int initialListenerSize = listeners.size();

            for (ToRegister<Seekables<?, ?>> toRegister : multiRegistrations)
            {
                CommandsForKeys.register(safeStore, updated, toRegister.value, toRegister.slice);
                addListener(CommandsForKey.listener(toRegister.value));
            }

            for (ToRegister<Seekable> toRegister : singleRegistrations)
            {
                CommandsForKeys.register(safeStore, updated, toRegister.value, toRegister.slice);
                addListener(CommandsForKey.listener(toRegister.value));
            }

            if (listeners.size() > initialListenerSize)
            {
                Command preUpdate = updated;
                updated = (T) Command.updateAttributes(updated, this);
                if (updated != preUpdate)
                {
                    preUpdate.markInvalidated();
                    updated.markActive();
                }
            }

            if (command != null)
                command.markSuperseded();

            safeStore.completeUpdate(this, command, updated);
            completed = true;

            return updated;
        }

        public Command updateAttributes()
        {
            return complete(Command.updateAttributes(command, this));
        }

        public Preaccepted preaccept(Timestamp executeAt, Ballot ballot)
        {
            if (command.status() == Status.NotWitnessed)
            {
                return complete(Preaccepted.Factory.create(this, executeAt, ballot));
            }
            else if (command.status() == Status.AcceptedInvalidate && command.executeAt() == null)
            {
                Accepted accepted = command.asAccepted();
                return complete(Accepted.Factory.create(this, accepted.saveStatus(), executeAt, accepted.promised(), accepted.accepted()));
            }
            else
            {
                Invariants.checkState(command.status() == Status.Accepted);
                return (Preaccepted) complete(Command.updateAttributes(command, this));
            }
        }

        public Accepted markDefined(Ballot promised)
        {
            Invariants.checkState(command.hasBeen(Status.AcceptedInvalidate));
            if (isSameClass(command, Accepted.class))
                return complete(Accepted.Factory.update(command.asAccepted(), this, SaveStatus.get(command.status(), DefinitionOnly), promised));
            return (Accepted) complete(Command.updateAttributes(command, this));
        }

        public Command updatePromised(Ballot promised)
        {
            return complete(Command.updateAttributes(command, this, promised));
        }

        public Accepted accept(Timestamp executeAt, Ballot ballot)
        {
            return complete(new Accepted(this, SaveStatus.Accepted, executeAt, ballot, ballot));
        }

        public Accepted acceptInvalidated(Ballot ballot)
        {
            Timestamp executeAt = command.isWitnessed() ? command.asWitnessed().executeAt() : null;
            return complete(new Accepted(this, SaveStatus.AcceptedInvalidate, executeAt, ballot, ballot));
        }

        public Committed commit(Timestamp executeAt, WaitingOn waitingOn)
        {
            return complete(Committed.Factory.create(this, SaveStatus.Committed, executeAt, command.promised(), command.accepted(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply));
        }

        public Command precommit(Timestamp executeAt)
        {
            throw new UnsupportedOperationException("TODO: figure out what this should be");
        }

        public Committed commitInvalidated(Timestamp executeAt)
        {
            return complete(Executed.Factory.create(this, SaveStatus.Invalidated, executeAt, command.promised(), command.accepted(), WaitingOn.EMPTY, null, null));
        }

        public Committed readyToExecute()
        {
            return complete(Committed.Factory.update(command.asCommitted(), this, SaveStatus.ReadyToExecute));
        }

        public Executed preapplied(Timestamp executeAt, WaitingOn waitingOn, Writes writes, Result result)
        {
            return complete(Executed.Factory.create(this, SaveStatus.PreApplied, executeAt, command.promised(), command.accepted(), waitingOn, writes, result));
        }

        public Committed noopApplied()
        {
            return complete(Executed.Factory.update(command.asExecuted(), this, SaveStatus.Applied));
        }

        public Executed applied()
        {
            return complete(Executed.Factory.update(command.asExecuted(), this, SaveStatus.Applied));
        }
    }
}
