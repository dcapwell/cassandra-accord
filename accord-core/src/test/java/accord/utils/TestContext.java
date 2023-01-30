package accord.utils;

import accord.api.Key;
import accord.local.Command;
import accord.impl.CommandsForKey;
import accord.local.PreExecuteContext;
import accord.primitives.Keys;
import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import java.util.HashMap;
import java.util.Map;

public class TestContext implements PreExecuteContext
{
    private final Map<TxnId, Command> commands = new HashMap<>();
    private final Map<RoutableKey, CommandsForKey> commandsForKey = new HashMap<>();

    public void add(Command command)
    {
        commands.put(command.txnId(), command);
    }

    public void addEmpty(TxnId txnId)
    {
        commands.put(txnId, Command.EMPTY);
    }

    public void addTxnIds(Iterable<TxnId> txnIds)
    {
        txnIds.forEach(this::addEmpty);
    }

    public void add(CommandsForKey cfk)
    {
        commandsForKey.put(cfk.key(), cfk);
    }

    public void addEmpty(RoutableKey key)
    {
        commandsForKey.put(key, CommandsForKey.EMPTY);
    }

    public void addKeys(Iterable<RoutableKey> keys)
    {
        keys.forEach(this::addEmpty);
    }

    public void addKeys(Seekables<?, ?> keys)
    {
        keys.forEach(k -> addEmpty((RoutableKey) k));
    }

    @Override
    public Map<TxnId, Command> commands()
    {
        return commands;
    }

    @Override
    public Map<RoutableKey, CommandsForKey> commandsForKey()
    {
        return commandsForKey;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return commands.keySet();
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.of((Key) commandsForKey.keySet());
    }
}
