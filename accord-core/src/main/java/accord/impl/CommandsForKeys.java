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
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandsForKeys
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    private CommandsForKeys() {}

    public static CommandListener register(AbstractSafeCommandStore safeStore, Command command, RoutableKey key, Ranges slice)
    {
        CommandsForKey cfk = safeStore.commandsForKey(key);
        CommandsForKey.Update update = safeStore.beginUpdate(cfk);
        update.updateMax(command.executeAt());
        update.byId().add(command.txnId(), command);
        update.byExecuteAt().add(command.txnId(), command);
        cfk = update.complete();
        return CommandsForKey.listener(cfk.key());
    }

    public static void listenerUpdate(SafeCommandStore safeStore, CommandsForKey listener, Command command)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                         listener.key(), command.txnId(), command.status(), command);

        CommandsForKey.Update update = safeStore.beginUpdate(listener);
        update.updateMax(command.executeAt());
        // add/remove the command on every listener update to avoid
        // special denormalization handling in Cassandra
        switch (command.status())
        {
            default: throw new AssertionError();
            case PreAccepted:
            case NotWitnessed:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                update.byId().add(command.txnId(), command);
                update.byExecuteAt().add(command.txnId(), command);
                break;
            case Applied:
            case PreApplied:
            case Committed:
            case ReadyToExecute:
                update.byId().add(command.txnId(), command);
                update.byExecuteAt().remove(command.txnId());
                update.byExecuteAt().add(command.executeAt(), command);
                break;
            case Invalidated:
                update.byId().remove(command.txnId());
                update.byExecuteAt().remove(command.txnId());
                break;
        }
        update.complete();
    }

}
