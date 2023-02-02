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
