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

import accord.api.Key;
import accord.local.*;
import accord.primitives.*;
import accord.utils.Invariants;
import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.function.Function;

public abstract class AbstractSafeCommandStore implements SafeCommandStore
{
    private static class PendingRegistration<T>
    {
        final T value;
        final Ranges slice;
        final TxnId txnId;

        public PendingRegistration(T value, Ranges slice, TxnId txnId)
        {
            this.value = value;
            this.slice = slice;
            this.txnId = txnId;
        }
    }
    protected final PreExecuteContext context;
    protected final Map<TxnId, LiveCommand> commands;
    protected final ContextState<RoutableKey, CommandsForKey, CommandsForKey.Update> commandsForKey;

    private List<PendingRegistration<Seekable>> pendingSeekableRegistrations = null;
    private List<PendingRegistration<Seekables<?, ?>>> pendingSeekablesRegistrations = null;

    public AbstractSafeCommandStore(PreExecuteContext context)
    {
        this.context = context;
        this.commands = new HashMap<>(context.commands());
        this.commandsForKey = new ContextState<>(CommandsForKey.EMPTY, context.commandsForKey());
    }

    public Map<TxnId, LiveCommand> commands()
    {
        return commands;
    }

    public ContextState<RoutableKey, CommandsForKey, CommandsForKey.Update> commandsForKey()
    {
        return commandsForKey;
    }

    @Override
    public LiveCommand ifPresent(TxnId txnId)
    {
        LiveCommand command = getIfLoaded(txnId, commands, this::getIfLoaded);
        if (command == null)
            return null;
        return command;
    }

    protected abstract LiveCommand getIfLoaded(TxnId txnId);

    private static <K, V extends ImmutableState> V maybeConvertEmpty(K key, V value, ContextState<K, V, ?> context, Function<K, V> factory, V emptySentinel)
    {
        if (value == emptySentinel)
        {
            value = factory.apply(key);
            value.checkIsDormant();
            value.markActive();
            context.setCurrent(key, value);
        }
        return value;
    }


    private static <K, V extends ImmutableState> V getIfLoaded(K key, ContextState<K, V, ?> context, Function<K, V> getIfLoaded, V emptySentinel)
    {
        V value = context.get(key);
        if (value != null)
            return value;

        value = getIfLoaded.apply(key);
        if (value == null)
            return null;

        if (value != emptySentinel)
        {
            value.checkIsDormant();
            value.markActive();
        }
        context.addOriginal(key, value);
        return value;
    }

    private static <K, V extends LiveState<?>> V getIfLoaded(K key, Map<K, V> context, Function<K, V> getIfLoaded)
    {
        V value = context.get(key);
        if (value != null)
            return value;

        value = getIfLoaded.apply(key);
        if (value == null)
            return null;
        context.put(key, value);
        return value;
    }

    @Override
    public LiveCommand ifLoaded(TxnId txnId)
    {
        LiveCommand command = getIfLoaded(txnId, commands, this::getIfLoaded);
        if (command == null)
            return null;
        if (command.current() == null)
            command.notWitnessed();
        return command;
    }

    @Override
    public LiveCommand command(TxnId txnId)
    {
        LiveCommand command = commands.get(txnId);
        if (command == null)
            throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", txnId));
        if (command.current() == null)
            command.notWitnessed();
        return command;
    }

    public CommandsForKey ifLoaded(RoutableKey key)
    {
        CommandsForKey cfk = getIfLoaded(key, commandsForKey, this::getIfLoaded, CommandsForKey.EMPTY);
        if (cfk == null)
            return null;
        cfk = maybeConvertEmpty(key, cfk, commandsForKey, k -> new CommandsForKey((Key) k, cfkLoader()), CommandsForKey.EMPTY);
        cfk.checkIsActive();
        return cfk;
    }

    public CommandsForKey commandsForKey(RoutableKey key)
    {
        CommandsForKey cfk = commandsForKey.get(key);
        if (cfk == null)
            throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", key));
        cfk = maybeConvertEmpty(key, cfk, commandsForKey, k -> new CommandsForKey((Key) k, cfkLoader()), CommandsForKey.EMPTY);
        cfk.checkIsActive();
        return cfk;
    }

    protected abstract CommandsForKey getIfLoaded(RoutableKey key);

    public CommandsForKey maybeCommandsForKey(RoutableKey key)
    {
        CommandsForKey cfk = getIfLoaded(key, commandsForKey, this::getIfLoaded, CommandsForKey.EMPTY);
        if (cfk == CommandsForKey.EMPTY)
            return null;
        return cfk;
    }

    @Override
    public boolean canExecuteWith(PreLoadContext context)
    {
        return context.isSubsetOf(this.context);
    }

    @Override
    public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
    {
        if (pendingSeekablesRegistrations == null)
            pendingSeekablesRegistrations = new ArrayList<>();
        pendingSeekablesRegistrations.add(new PendingRegistration<>(keysOrRanges, slice, command.txnId()));
    }

    @Override
    public void register(Seekable keyOrRange, Ranges slice, Command command)
    {
        if (pendingSeekableRegistrations == null)
            pendingSeekableRegistrations = new ArrayList<>();
        pendingSeekableRegistrations.add(new PendingRegistration<>(keyOrRange, slice, command.txnId()));
    }

    protected abstract Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice);

    @Override
    public Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys)
    {
        Timestamp max = maxConflict(keys, ranges().at(txnId.epoch()));
        long epoch = latestEpoch();
        long now = time().now();
        if (txnId.compareTo(max) > 0 && txnId.epoch() >= epoch && !agent().isExpired(txnId, now))
            return txnId;

        return time().uniqueNow(max);
    }

    protected CommandsForKey.Update createCommandsForKeyUpdate(CommandsForKey cfk)
    {
        return new CommandsForKey.Update(this, cfk);
    }

    @Override
    public CommandsForKey.Update beginUpdate(CommandsForKey cfk)
    {
        return commandsForKey.beginUpdate(cfk.key(), cfk, this::createCommandsForKeyUpdate);
    }

    @Override
    public void completeUpdate(CommandsForKey.Update update, CommandsForKey current, CommandsForKey updated)
    {
        commandsForKey.completeUpdate(update.key(), update, current, updated);
    }

    abstract CommonAttributes completeRegistration(Seekables<?, ?> keysOrRanges, Ranges slice, LiveCommand command, CommonAttributes attrs);

    abstract CommonAttributes completeRegistration(Seekable keyOrRange, Ranges slice, LiveCommand command, CommonAttributes attrs);

    private interface RegistrationCompleter<T>
    {
        CommonAttributes complete(T value, Ranges ranges, LiveCommand command, CommonAttributes attrs);
    }

    private <T> void completeRegistrations(Map<TxnId, CommonAttributes> updates, List<PendingRegistration<T>> pendingRegistrations, RegistrationCompleter<T> completer)
    {
        if (pendingRegistrations == null)
            return;

        for (PendingRegistration<T> pendingRegistration : pendingRegistrations)
        {
            TxnId txnId = pendingRegistration.txnId;
            LiveCommand liveCommand = command(pendingRegistration.txnId);
            Command command = liveCommand.current();
            CommonAttributes attrs = updates.getOrDefault(txnId, command);
            attrs = completer.complete(pendingRegistration.value, pendingRegistration.slice, liveCommand, attrs);
            if (attrs != command)
                updates.put(txnId, attrs);
        }
    }

    @Override
    public PostExecuteContext complete()
    {
        if (pendingSeekableRegistrations != null || pendingSeekablesRegistrations != null)
        {
            Map<TxnId, CommonAttributes> attributeUpdates = new HashMap<>();
            completeRegistrations(attributeUpdates, pendingSeekablesRegistrations, this::completeRegistration);
            completeRegistrations(attributeUpdates, pendingSeekableRegistrations, this::completeRegistration);
            attributeUpdates.forEach(((txnId, attributes) -> command(txnId).updateAttributes(attributes)));
        }
        return new PostExecuteContext(ImmutableMap.copyOf(commands), commandsForKey.complete());
    }

    public static class ContextState<Key, Value extends ImmutableState, Update>
    {
        private final Value emptySentinel;
        private final Map<Key, ContextValue.WithUpdate<Value, Update>> values;
        private final Set<Key> pendingUpdates = new HashSet<>();
        private boolean completed = false;

        public ContextState(Value emptySentinel, Map<Key, Value> originals)
        {
            this.emptySentinel = emptySentinel;
            this.values = new HashMap<>();
            originals.forEach((k, v) -> {
                if (v != emptySentinel)
                {
                    v.checkIsDormant();
                    v.markActive();
                }
                values.put(k, new ContextValue.WithUpdate<>(v));
            });
        }

        private void checkState()
        {
            if (completed)
                throw new IllegalStateException("Context has been closed");
        }

        private Value getOriginal(Key key)
        {
            Value val = ContextValue.original(values.get(key));
            return val != emptySentinel ? val : null;
        }

        private Value getCurrent(Key key)
        {
            Value val = ContextValue.current(values.get(key));
            return val != emptySentinel ? val : null;
        }

        public Value get(Key key)
        {
            checkState();
            return ContextValue.current(values.get(key));
        }

        public void addOriginal(Key key, Value item)
        {
            Invariants.checkArgument(!values.containsKey(key));
            values.put(key, new ContextValue.WithUpdate<>(item));
        }

        public void setCurrent(Key key, Value current)
        {
            ContextValue.WithUpdate<Value, Update> value = values.get(key);
            Invariants.checkState(value != null && value.current() == emptySentinel);
            value.current(current);
        }

        public Update beginUpdate(Key key, Value item, Function<Value, Update> factory)
        {
            checkState();
            ContextValue.WithUpdate<Value, Update> value = values.get(key);
            if (value == null)
                throw new IllegalStateException("No state in context for " + key);
            if (value.current() != item)
                throw new IllegalStateException(String.format("Attempting to update invalid state for %s", key));

            if (value.update() != null)
                throw new IllegalStateException(String.format("Update already in progress for %s", key));

            Update update = factory.apply(item);
            value.update(update);
            pendingUpdates.add(key);
            return update;
        }

        public void completeUpdate(Key key, Update update, Value prev, Value next)
        {
            checkState();
            ContextValue.WithUpdate<Value, Update> value = values.get(key);
            if (value == null)
                throw new IllegalStateException("No state in context for " + key);
            if (prev != null) prev.checkIsSuperseded();
            if (next != null) next.checkIsCurrent();
            Value currentVal = value.current();
            if (currentVal == emptySentinel)
                currentVal = null;
            Update pending = value.update();
            if (pending == null)
                throw new IllegalStateException(String.format("Update doesn't exist for %s", key));
            if (pending != update)
                throw new IllegalStateException(String.format("Update was already completed for %s", key));

            value.clearUpdate();
            pendingUpdates.remove(key);
            if (currentVal != prev)
                throw new IllegalStateException(String.format("Current value for %s differs from expected value (%s != %s)", key, value.current(), prev));
            value.current(next);
        }

        public ImmutableMap<Key, ContextValue<Value>> complete()
        {
            checkState();
            if (!pendingUpdates.isEmpty())
                throw new IllegalStateException("Pending updates left uncompleted: " + pendingUpdates);

            ImmutableMap.Builder<Key, ContextValue<Value>> result = ImmutableMap.builder();
            values.forEach((key, v) -> {
                if (v.update() != null)
                    throw new IllegalStateException("Pending update left uncompleted for " + key);

                Value original = v.original();
                if (original == emptySentinel)
                    original = null;

                Value current = v.current();
                if (current == emptySentinel)
                    current = null;

                // we sometimes specify cfks in our preload context that we don't end up updating
                // if they're out of range, so we skip writing them into state here
                if (original != null || current != null)
                    result.put(key, new ContextValue<>(original, current));
            });
            completed = true;
            return result.build();
        }

        public void checkActive()
        {
            values.forEach((k, v) -> {
                if (v.current() != emptySentinel)
                    v.current().checkIsActive();
            });
        }

        public void markDormant()
        {
            values.forEach((k, v) -> {
                if (v.current() != emptySentinel)
                    v.current().markDormant();
            });
        }
    }
}
