package accord.local;

public class StateUpdate<V, U extends StateUpdate<V, U>>
{
    public interface CompletionCallback<V, U extends StateUpdate<V, U>>
    {
        void onCompletion(U update, V previous, V next);
    }

    private enum State { UPDATING, BUILDING, COMPLETED }

    private State state = State.UPDATING;
    protected final V original;
    private final CompletionCallback<V, U> callback;

    public StateUpdate(V original, CompletionCallback<V, U> callback)
    {
        this.original = original;
        this.callback = callback;
    }

    protected void checkCanRead()
    {
        switch (state)
        {
            case UPDATING:
            case BUILDING:
                return;
            default:
                throw new IllegalStateException("Cannot read values with state: " + state);
        }
    }

    protected void checkCanWrite()
    {
        switch (state)
        {
            case UPDATING:
                return;
            default:
                throw new IllegalStateException("Cannot read values with state: " + state);
        }
    }

    protected void setStateBuilding()
    {
        if (state != State.UPDATING)
            throw new IllegalStateException("Cannot transition to " + State.BUILDING + " from " + state);
        state = State.BUILDING;
    }

    protected <T extends V> T preComplete(T updated)
    {
        return updated;
    }

    protected <T extends V> T postComplete(T updated)
    {
        return updated;
    }

    protected  <T extends V> T complete(T updated)
    {
        if (state != State.BUILDING)
            throw new IllegalStateException("Cannot transition to " + State.BUILDING + " from " + state);

        if (updated == original)
            throw new IllegalStateException("Update is the same as the original");

        updated = preComplete(updated);

        callback.onCompletion((U) this, original, updated);
        state = State.COMPLETED;
        return postComplete(updated);
    }

}
