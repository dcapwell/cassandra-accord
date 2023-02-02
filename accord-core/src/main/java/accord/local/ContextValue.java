package accord.local;

public class ContextValue<Value>
{
    private final Value original;
    private Value current;

    public static <V> V original(ContextValue<V> value)
    {
        return value != null ? value.original : null;
    }

    public static <V> V current(ContextValue<V> value)
    {
        return value != null ? value.current : null;
    }

    public ContextValue(Value original, Value current)
    {
        this.original = original;
        this.current = current;
    }

    public ContextValue(Value original)
    {
        this(original, original);
    }

    public Value original()
    {
        return original;
    }

    public Value current()
    {
        return current;
    }

    public ContextValue<Value> current(Value current)
    {
        this.current = current;
        return this;
    }

    public static class WithUpdate<Value, Update> extends ContextValue<Value>
    {
        private Update update;

        public WithUpdate(Value original)
        {
            super(original);
            this.update = null;
        }

        public Update update()
        {
            return update;
        }

        public WithUpdate<Value, Update> update(Update update)
        {
            this.update = update;
            return this;
        }

        public void clearUpdate()
        {
            update = null;
        }
    }
}
