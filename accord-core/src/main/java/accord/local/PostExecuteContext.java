package accord.local;

import accord.impl.CommandsForKey;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import com.google.common.collect.ImmutableMap;

public class PostExecuteContext
{
    public final ImmutableMap<TxnId, ContextValue<Command>> commands;
    public final ImmutableMap<RoutableKey, ContextValue<CommandsForKey>> commandsForKey;

    public PostExecuteContext(ImmutableMap<TxnId, ContextValue<Command>> commands, ImmutableMap<RoutableKey, ContextValue<CommandsForKey>> commandsForKey)
    {
        this.commands = commands;
        this.commandsForKey = commandsForKey;
    }
}
