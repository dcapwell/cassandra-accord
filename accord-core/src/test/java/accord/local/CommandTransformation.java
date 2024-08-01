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

import javax.annotation.Nullable;

public interface CommandTransformation
{
    enum Status {Ok, Done, Ignore}

    class Result
    {
        final Status status;
        @Nullable
        final Command command;

        public Result(Status status, @Nullable Command command)
        {
            this.status = status;
            this.command = command;
        }

        public static Result ok(@Nullable Command command)
        {
            return new Result(Status.Ok, command);
        }

        public static Result done(@Nullable Command command)
        {
            return new Result(Status.Done, command);
        }

        public static Result ignore(@Nullable Command command)
        {
            return new Result(Status.Ignore, command);
        }
    }
    
    Result transform(SafeCommandStore cs);

    class NamedCommandTransformation implements CommandTransformation
    {
        public final String name;
        private final CommandTransformation delegate;

        public NamedCommandTransformation(String name, CommandTransformation delegate)
        {
            this.name = name;
            this.delegate = delegate;
        }

        @Override
        public Result transform(SafeCommandStore cs)
        {
            return delegate.transform(cs);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
