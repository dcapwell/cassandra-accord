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

import accord.local.Command;
import accord.local.LiveCommand;
import accord.primitives.TxnId;

public class InMemoryLiveCommand extends LiveCommand
{
    private volatile Command current = null;

    public InMemoryLiveCommand(TxnId txnId, Command current)
    {
        super(txnId);
        this.current = current;
    }

    public InMemoryLiveCommand(TxnId txnId)
    {
        this(txnId, null);
    }

    @Override
    public Command current()
    {
        return current;
    }

    @Override
    protected void set(Command update)
    {
        this.current = update;
    }
}
