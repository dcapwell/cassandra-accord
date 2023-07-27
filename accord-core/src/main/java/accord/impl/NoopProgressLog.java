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

import accord.api.ProgressLog;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.SafeCommand;
import accord.local.SaveStatus;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;

import javax.annotation.Nullable;

public enum NoopProgressLog implements ProgressLog.Factory
{
    INSTANCE;

    @Override
    public ProgressLog create(CommandStore store)
    {
        return Instance.INSTANCE;
    }

    private enum Instance implements ProgressLog
    {
        INSTANCE;
        @Override
        public void unwitnessed(TxnId txnId, ProgressShard shard)
        {

        }

        @Override
        public void preaccepted(Command command, ProgressShard shard)
        {

        }

        @Override
        public void accepted(Command command, ProgressShard shard)
        {

        }

        @Override
        public void committed(Command command, ProgressShard shard)
        {

        }

        @Override
        public void readyToExecute(Command command)
        {

        }

        @Override
        public void executed(Command command, ProgressShard shard)
        {

        }

        @Override
        public void durable(Command command)
        {

        }

        @Override
        public void waiting(SafeCommand blockedBy, SaveStatus.LocalExecution blockedUntil, @Nullable Route<?> blockedOnRoute, @Nullable Participants<?> blockedOnParticipants)
        {

        }

        @Override
        public void clear(TxnId txnId)
        {

        }
    }
}
