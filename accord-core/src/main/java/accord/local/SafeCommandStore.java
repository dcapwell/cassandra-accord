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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.impl.CommandsForKey;
import accord.primitives.*;
import accord.api.*;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncCallbacks;

import javax.annotation.Nullable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.Comparator;
import java.util.Objects;

import static accord.utils.Utils.listOf;

/**
 * A CommandStore with exclusive access; a reference to this should not be retained outside of the scope of the method
 * that it is passed to. For the duration of the method invocation only, the methods on this interface are safe to invoke.
 *
 * Method implementations may therefore be single threaded, without volatile access or other concurrency control
 */
public interface SafeCommandStore
{
    Command ifPresent(TxnId txnId);

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    Command ifLoaded(TxnId txnId);
    Command command(TxnId txnId);

    CommandsForKey commandsForKey(RoutableKey key);
    CommandsForKey maybeCommandsForKey(RoutableKey key);

    boolean canExecuteWith(PreLoadContext context);

    /**
     * Register a listener against the given TxnId, then load the associated transaction and invoke the listener
     * with its current state.
     */
    default void addAndInvokeListener(TxnId txnId, TxnId listenerId)
    {
        PreLoadContext context = PreLoadContext.contextFor(listOf(txnId, listenerId), Keys.EMPTY);
        commandStore().execute(context, safeStore -> {
            Command command = safeStore.command(txnId);
            CommandListener listener = Command.listener(listenerId);
            Command.addListener(safeStore, command, listener);
            listener.onChange(safeStore, txnId);
        }).begin(AsyncCallbacks.noop());
    }

    interface CommandFunction<I, O>
    {
        O apply(Seekable keyOrRange, TxnId txnId, Timestamp executeAt, I in);
    }

    enum TestTimestamp
    {
        STARTED_BEFORE,
        STARTED_AFTER,
        MAY_EXECUTE_BEFORE, // started before and uncommitted, or committed and executes before
        EXECUTES_AFTER
    }
    enum TestDep { WITH, WITHOUT, ANY_DEPS }
    enum TestKind { Ws, RorWs }

    /**
     * Visits keys first and then ranges, both in ascending order.
     * Within each key or range visits TxnId in ascending order of queried timestamp.
     */
    <T> T mapReduce(Seekables<?, ?> keys, Ranges slice,
                    TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                    TestDep testDep, @Nullable TxnId depId,
                    @Nullable Status minStatus, @Nullable Status maxStatus,
                    CommandFunction<T, T> map, T initialValue, T terminalValue);

    CommandStore commandStore();
    DataStore dataStore();
    Agent agent();
    ProgressLog progressLog();
    NodeTimeService time();
    CommandStores.RangesForEpoch ranges();
    long latestEpoch();
    Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys);

    default void notifyListeners(Command command)
    {
        TxnId txnId = command.txnId();
        for (CommandListener listener : command.listeners())
        {
            PreLoadContext context = listener.listenerPreLoadContext(command.txnId());
            if (canExecuteWith(context))
            {
                listener.onChange(this, txnId);
            }
            else
            {
                commandStore().execute(context, safeStore -> listener.onChange(safeStore, txnId)).begin(AsyncCallbacks.noop());
            }
        }
    }

    Command.Update beginUpdate(Command command);

    default Command.Update beginUpdate(TxnId txnId)
    {
        return beginUpdate(command(txnId));
    }

    void completeUpdate(Command.Update update, Command current, Command updated);

    CommandsForKey.Update beginUpdate(CommandsForKey commandsForKey);

    void completeUpdate(CommandsForKey.Update update, CommandsForKey current, CommandsForKey updated);

    PostExecuteContext complete();

    CommandsForKey.CommandLoader<?> cfkLoader();
}
