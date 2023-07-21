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

import accord.api.VisibleForImplementation;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.utils.Invariants;

public interface CommonAttributes
{
    TxnId txnId();
    Status.Durability durability();
    Route<?> route();
    PartialTxn partialTxn();
    PartialDeps partialDeps();
    Listeners.Immutable durableListeners();

    default Mutable mutable()
    {
        return new Mutable(this);
    }

    class Mutable implements CommonAttributes
    {
        // TODO review these are global ones are mutable
        public static final CommonAttributes.Mutable EMPTY_ATTRS = new CommonAttributes.Mutable((TxnId) null);
        public static final CommonAttributes.Mutable EMPTY_ATTRS_NONE_PARTIAL_DEPS = new Mutable((TxnId) null).partialDeps(PartialDeps.NONE);
        private TxnId txnId;
        private Status.Durability durability;
        private Route<?> route;
        private PartialTxn partialTxn;
        private PartialDeps partialDeps;
        private Listeners listeners;

        public Mutable(TxnId txnId)
        {
            this.txnId = txnId;
        }

        public Mutable(CommonAttributes attributes)
        {
            this.txnId = attributes.txnId();
            this.durability = attributes.durability();
            this.route = attributes.route();
            this.partialTxn = attributes.partialTxn();
            this.partialDeps = attributes.partialDeps();
            this.listeners = attributes.durableListeners();
        }

        @Override
        public Mutable mutable()
        {
            return this;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        public Mutable txnId(TxnId txnId)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            this.txnId = txnId;
            return this;
        }

        @Override
        public Status.Durability durability()
        {
            return durability;
        }

        public Mutable durability(Status.Durability durability)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            this.durability = durability;
            return this;
        }

        @Override
        public Route<?> route()
        {
            return route;
        }

        public Mutable route(Route<?> route)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            this.route = route;
            return this;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        public Mutable partialTxn(PartialTxn partialTxn)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            this.partialTxn = partialTxn;
            return this;
        }

        @Override
        public PartialDeps partialDeps()
        {
            return partialDeps;
        }

        public Mutable partialDeps(PartialDeps partialDeps)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            this.partialDeps = partialDeps;
            return this;
        }

        @Override
        public Listeners.Immutable durableListeners()
        {
            if (listeners == null || listeners.isEmpty())
                return Listeners.Immutable.EMPTY;
            if (listeners instanceof Listeners.Immutable)
                return (Listeners.Immutable) listeners;
            return new Listeners.Immutable(listeners);
        }

        public Mutable addListener(Command.DurableAndIdempotentListener listener)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            if (listeners == null)
                listeners = new Listeners();
            else if (listeners instanceof Listeners.Immutable)
                listeners = new Listeners(listeners);
            listeners.add(listener);
            return this;
        }

        public Mutable removeListener(Command.Listener listener)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            if (listener == null || listeners.isEmpty())
                return this;
            if (listeners instanceof Listeners.Immutable)
                listeners = new Listeners(listeners);
            listeners.remove(listener);
            return this;
        }

        @VisibleForImplementation
        public Mutable setListeners(Listeners.Immutable listeners)
        {
            Invariants.checkState(this != EMPTY_ATTRS && this != EMPTY_ATTRS_NONE_PARTIAL_DEPS, "Don't mutate the global empty instances");
            this.listeners = listeners;
            return this;
        }
    }
}
