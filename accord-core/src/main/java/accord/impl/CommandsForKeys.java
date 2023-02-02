package accord.impl;

import accord.api.Key;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandsForKeys
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    private CommandsForKeys() {}

    public static boolean register(SafeCommandStore safeStore, CommandsForKey cfk, Command command)
    {
        boolean listenToCommand = false;
        CommandsForKey.Update update = safeStore.beginUpdate(cfk);
        update.updateMax(command.executeAt());

        if (command.hasBeen(Status.Committed))
        {
            update.addCommitted(command);
        }
        else if (command.status() != Status.Invalidated)
        {
            update.addUncommitted(command);
            listenToCommand = true;
        }
        update.complete();
        return listenToCommand;
    }

    public static boolean register(SafeCommandStore safeStore, Command command, Seekable keyOrRange, Ranges slice)
    {
        throw new UnsupportedOperationException("TODO");
    }

    public static boolean register(SafeCommandStore safeStore, Command command, Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        throw new UnsupportedOperationException("TODO");
    }

    static void register(SafeCommandStore safeStore, Key key, Command command)
    {
        register(safeStore, safeStore.commandsForKey(key), command);
    }

    public static void listenerUpdate(SafeCommandStore safeStore, CommandsForKey listener, Command command)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                         listener.key(), command.txnId(), command.status(), command);

        CommandsForKey.Update update = safeStore.beginUpdate(listener);
        update.updateMax(command.executeAt());
        switch (command.status())
        {
            default:
                throw new AssertionError();
            case PreAccepted:
            case NotWitnessed:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                update.addUncommitted(command);
                break;
            case Applied:
            case PreApplied:
            case Committed:
            case ReadyToExecute:
                update.addCommitted(command);
            case Invalidated:
                update.removeUncommitted(command);
                Command.removeListener(safeStore, command, CommandsForKey.listener(listener.key()));
                break;
        }
        update.complete();
    }

}
