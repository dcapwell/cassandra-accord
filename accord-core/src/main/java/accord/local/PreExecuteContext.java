package accord.local;

import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import java.util.Map;

public interface PreExecuteContext extends PreLoadContext
{
    Map<TxnId, Command> commands();
    Map<RoutableKey, CommandsForKey> commandsForKey();

    static PreExecuteContext of(PreLoadContext preLoadContext, Map<TxnId, Command> commands, Map<RoutableKey, CommandsForKey> commandsForKey)
    {
        return new PreExecuteContext()
        {
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
                return preLoadContext.txnIds();
            }

            @Override
            public Seekables<?, ?> keys()
            {
                return preLoadContext.keys();
            }
        };
    }
}
