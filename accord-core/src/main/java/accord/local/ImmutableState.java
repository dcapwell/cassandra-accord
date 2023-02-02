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

/**
 * Since metadata instances are immutable, there can be several copies floating around in memory, all but one
 * of which is stale. This can lead to difficult to diagnose bugs, especially when an implementation side cache
 * is involved.
 * The ImmutableState base class defines the various lifecycle stage and helps restrict what the user does with it,
 * with the goal or throwing an exception as soon as an illegal usage occurs, instead of after.
 *
 * An up to date instances can have one of 2 statuses, DORMANT or ACTIVE. An instance is only ACTIVE in the time
 * between when the safe store for it's current operation is created, and closed, and is otherwise marked DORMANT.
 * While DORMANT, it cannot be read from or updated. This is to help detect leaked instances being read from or
 * updated when they shouldn't be.
 *
 * Once an update has created a superseding instance, the original instance is marked SUPERSEDED. It can then be
 * marked CLEANING_UP, during which time its fields can be read to assist in determining what has been updated.
 * From CLEANING_UP, an instance can be marked INVALIDATED to indicate that any access is illegal. An instance
 * marked SUPERSEDED can also go directly to INVALIDATED, in the case of intermediate updates.
 */
public abstract class ImmutableState
{
    public enum State
    {
        DORMANT,
        ACTIVE,
        SUPERSEDED,
        CLEANING_UP,
        INVALIDATED;

        private boolean canTransitionTo(State next)
        {
            switch (next)
            {
                case DORMANT:
                case SUPERSEDED:
                    return this == ACTIVE;
                case ACTIVE:
                    return this == DORMANT;
                case CLEANING_UP:
                    return this == SUPERSEDED;
                case INVALIDATED:
                    return true;
                default:
                    throw new IllegalArgumentException("Unhandled state: " + next);
            }
        }

        private boolean canReadFrom()
        {
            switch (this)
            {
                case ACTIVE:
                case CLEANING_UP:
                    return true;
                default:
                    return false;
            }
        }

        private boolean canUpdate()
        {
            return this == ACTIVE;
        }

        private boolean isCurrent()
        {
            switch (this)
            {
                case DORMANT:
                case ACTIVE:
                    return true;
                default:
                    return false;
            }
        }
    }

    private State state = State.DORMANT;

    private static void illegalState(String format, Object... args)
    {
        throw new IllegalArgumentException(String.format(format, args));
    }

    private void setState(State next)
    {
        if (!state.canTransitionTo(next))
            illegalState("Cannot transition from %s to %s for %s", state, next, this);
        state = next;
    }

    private void checkState(State expected)
    {
        if (state != expected)
            illegalState("Expected state %s, but was %s for %s", expected, state, this);
    }

    public void markDormant()
    {
        setState(State.DORMANT);
    }

    public void checkIsDormant()
    {
        checkState(State.DORMANT);
    }

    public boolean isDormant()
    {
        return state == State.DORMANT;
    }

    public void markActive()
    {
        setState(State.ACTIVE);
    }

    public void markActiveIfDormant()
    {
        if (state == State.DORMANT)
            markActive();
        else
            checkIsActive();
    }

    public void checkIsActive()
    {
        checkState(State.ACTIVE);
    }

    public void markSuperseded()
    {
        setState(State.SUPERSEDED);
    }

    public void markCleaningUp()
    {
        setState(State.CLEANING_UP);
    }

    public void markInvalidated()
    {
        setState(State.INVALIDATED);
    }

    public boolean canReadFrom()
    {
        return state.canReadFrom();
    }

    public void checkCanReadFrom()
    {
        if (!canReadFrom())
            illegalState("Unable to read from %s in state %s", this, state);
    }

    public boolean canUpdate()
    {
        return state.canUpdate();
    }

    public void checkCanUpdate()
    {
        if (!canUpdate())
            illegalState("Cannot start update for %s in state %s", this, state);
    }

    public boolean isCurrent()
    {
        return state.isCurrent();
    }

    public void checkIsCurrent()
    {
        if (!isCurrent())
            illegalState("State %s is not current (%s)", state, this);
    }

    public void checkIsSuperseded()
    {
        if (isCurrent())
            illegalState("State %s is not superseded (%s)", state, this);
    }
}
