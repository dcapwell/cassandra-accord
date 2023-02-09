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

import accord.api.Result;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.utils.Invariants;

import static accord.local.Status.Known.DefinitionOnly;

// FIXME: don't implement CommonAttributes
public abstract class LiveCommand extends LiveState<Command>
{
    public final boolean isWitnessed()
    {
        Command current = current();
        boolean result = current.status().hasBeen(Status.PreAccepted);
        Invariants.checkState(result == (current instanceof Command.Preaccepted));
        return result;
    }

    public final Command.Preaccepted asWitnessed()
    {
        return (Command.Preaccepted) current();
    }

    public final boolean isAccepted()
    {
        Command current = current();
        boolean result = current.status().hasBeen(Status.AcceptedInvalidate);
        Invariants.checkState(result == (current instanceof Command.Accepted));
        return result;
    }

    public final Command.Accepted asAccepted()
    {
        return (Command.Accepted) current();
    }

    public final boolean isCommitted()
    {
        Command current = current();
        boolean result = current.status().hasBeen(Status.Committed);
        Invariants.checkState(result == (current instanceof Command.Committed));
        return result;
    }

    public final Command.Committed asCommitted()
    {
        return (Command.Committed) current();
    }

    public final boolean isExecuted()
    {
        Command current = current();
        boolean result = current.status().hasBeen(Status.PreApplied);
        Invariants.checkState(result == (current instanceof Command.Executed));
        return result;
    }

    public final Command.Executed asExecuted()
    {
        return (Command.Executed) current();
    }

    private <C extends Command> C update(C update)
    {
        update(update);
        return update;
    }

    // FIXME: remove - just stick w/ update
    private <C extends Command> C complete(C update)
    {
        return update(update);
    }

    private static Command updateAttributes(Command command, CommonAttributes attributes, Ballot promised)
    {
        switch (command.status())
        {
            case NotWitnessed:
                return Command.NotWitnessed.Factory.update((Command.NotWitnessed) command, attributes, promised);
            case PreAccepted:
                return Command.Preaccepted.Factory.update((Command.Preaccepted) command, attributes, promised);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return Command.Accepted.Factory.update((Command.Accepted) command, attributes, promised);
            case Committed:
            case ReadyToExecute:
                return Command.Committed.Factory.update((Command.Committed) command, attributes, promised);
            case PreApplied:
            case Applied:
            case Invalidated:
                return Command.Executed.Factory.update((Command.Executed) command, attributes, promised);
            default:
                throw new IllegalStateException("Unhandled status " + command.status());
        }
    }

    private static Command updateAttributes(Command command, CommonAttributes attributes)
    {
        return updateAttributes(command, attributes, command.promised());
    }

    public Command addListener(CommandListener listener)
    {
        CommonAttributes attrs = current().mutableAttrs().addListener(listener);
        return complete(updateAttributes(current(), attrs));
    }

    public Command removeListener(CommandListener listener)
    {
        CommonAttributes attrs = current().mutableAttrs().removeListener(listener);
        return complete(updateAttributes(current(), attrs));
    }

    public Command.Committed updateWaitingOn(SafeCommandStore safeStore, Command.Committed command, Command.WaitingOn.Update waitingOn)
    {
        if (!waitingOn.hasChanges())
            return command;

        Command.Committed updated =  command instanceof Command.Executed ?
                Command.Executed.Factory.update(command.asExecuted(), command, waitingOn.build()) :
                Command.Committed.Factory.update(command, command, waitingOn.build());
        return complete(updated);
    }

    public Command updateAttributes(CommonAttributes attrs)
    {
        return complete(updateAttributes(current(), attrs));
    }

    public Command.Preaccepted preaccept(CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        if (current().status() == Status.NotWitnessed)
        {
            return complete(Command.Preaccepted.Factory.create(attrs, executeAt, ballot));
        }
        else if (current().status() == Status.AcceptedInvalidate && current().executeAt() == null)
        {
            Command.Accepted accepted = asAccepted();
            return complete(Command.Accepted.Factory.create(attrs, accepted.saveStatus(), executeAt, ballot, accepted.accepted()));
        }
        else
        {
            Invariants.checkState(current().status() == Status.Accepted);
            return (Command.Preaccepted) complete(updateAttributes(current(), attrs, ballot));
        }
    }

    public Command.Accepted markDefined(CommonAttributes attributes, Ballot promised)
    {
        if (Command.isSameClass(current(), Command.Accepted.class))
            return complete(Command.Accepted.Factory.update(asAccepted(), attributes, SaveStatus.enrich(current().saveStatus(), DefinitionOnly), promised));
        return (Command.Accepted) complete(updateAttributes(current(), attributes, promised));
    }

    public Command updatePromised(Ballot promised)
    {
        return complete(updateAttributes(current(), current(), promised));
    }

    public Command.Accepted accept(CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return complete(new Command.Accepted(attrs, SaveStatus.get(Status.Accepted, current().known()), executeAt, ballot, ballot));
    }

    public Command.Accepted acceptInvalidated(Ballot ballot)
    {
        Timestamp executeAt = isWitnessed() ? asWitnessed().executeAt() : null;
        return complete(new Command.Accepted(current(), SaveStatus.AcceptedInvalidate, executeAt, ballot, ballot));
    }

    public Command.Committed commit(CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn)
    {
        return complete(Command.Committed.Factory.create(attrs, SaveStatus.Committed, executeAt, current().promised(), current().accepted(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply));
    }

    public Command precommit(Timestamp executeAt)
    {
        return complete(new Command.Accepted(current(), SaveStatus.PreCommitted, executeAt, current().promised(), current().accepted()));
    }

    public Command.Committed commitInvalidated(CommonAttributes attrs, Timestamp executeAt)
    {
        return complete(Command.Executed.Factory.create(attrs, SaveStatus.Invalidated, executeAt, current().promised(), current().accepted(), Command.WaitingOn.EMPTY, null, null));
    }

    public Command.Committed readyToExecute()
    {
        return complete(Command.Committed.Factory.update(current().asCommitted(), current(), SaveStatus.ReadyToExecute));
    }

    public Command.Executed preapplied(CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return complete(Command.Executed.Factory.create(attrs, SaveStatus.PreApplied, executeAt, current().promised(), current().accepted(), waitingOn, writes, result));
    }

    public Command.Committed noopApplied()
    {
        return complete(Command.Executed.Factory.update(asExecuted(), current(), SaveStatus.Applied));
    }

    public Command.Executed applied()
    {
        return complete(Command.Executed.Factory.update(asExecuted(), current(), SaveStatus.Applied));
    }
}
