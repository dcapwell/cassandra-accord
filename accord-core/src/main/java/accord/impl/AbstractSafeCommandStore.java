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

import accord.impl.CommandsForKey.CommandLoader;
import accord.local.*;
import accord.primitives.*;
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
    protected final PreLoadContext context;
    protected final Map<TxnId, LiveCommand> commands;
    protected final Map<RoutableKey, LiveCommandsForKey> commandsForKey;

    private List<PendingRegistration<Seekable>> pendingSeekableRegistrations = null;
    private List<PendingRegistration<Seekables<?, ?>>> pendingSeekablesRegistrations = null;

    public AbstractSafeCommandStore(PreLoadContext context, Map<TxnId, LiveCommand> commands, Map<RoutableKey, LiveCommandsForKey> commandsForKey)
    {
        this.context = context;
        this.commands = commands;
        this.commandsForKey = commandsForKey;
    }

    public Map<TxnId, LiveCommand> commands()
    {
        return commands;
    }

    public Map<RoutableKey, LiveCommandsForKey> commandsForKey()
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
        if (command.isEmpty())
            command.notWitnessed();
        return command;
    }

    @Override
    public LiveCommand command(TxnId txnId)
    {
        LiveCommand command = commands.get(txnId);
        if (command == null)
            throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", txnId));
        if (command.isEmpty())
            command.notWitnessed();
        return command;
    }

    protected abstract CommandLoader<?> cfkLoader();

    public LiveCommandsForKey ifLoaded(RoutableKey key)
    {
        LiveCommandsForKey cfk = getIfLoaded(key, commandsForKey, this::getIfLoaded);
        if (cfk == null)
            return null;
        if (cfk.isEmpty())
            cfk.initialize(cfkLoader());
        return cfk;
    }

    public LiveCommandsForKey commandsForKey(RoutableKey key)
    {
        LiveCommandsForKey cfk = commandsForKey.get(key);
        if (cfk == null)
            throw new IllegalStateException(String.format("%s was not specified in PreLoadContext", key));
        if (cfk.isEmpty())
            cfk.initialize(cfkLoader());
        return cfk;
    }

    protected abstract LiveCommandsForKey getIfLoaded(RoutableKey key);

    public LiveCommandsForKey maybeCommandsForKey(RoutableKey key)
    {
        LiveCommandsForKey cfk = getIfLoaded(key, commandsForKey, this::getIfLoaded);
        if (cfk == null)
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

    public void complete()
    {
        if (pendingSeekableRegistrations != null || pendingSeekablesRegistrations != null)
        {
            Map<TxnId, CommonAttributes> attributeUpdates = new HashMap<>();
            completeRegistrations(attributeUpdates, pendingSeekablesRegistrations, this::completeRegistration);
            completeRegistrations(attributeUpdates, pendingSeekableRegistrations, this::completeRegistration);
            attributeUpdates.forEach(((txnId, attributes) -> command(txnId).updateAttributes(attributes)));
        }
    }
}
