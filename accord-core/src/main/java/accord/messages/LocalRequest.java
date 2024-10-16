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
package accord.messages;

import accord.local.Node;
import accord.primitives.TxnId;

import java.util.function.BiConsumer;
import javax.annotation.Nullable;

public interface LocalRequest<R> extends Message
{
    default long waitForEpoch() { return 0; }
    @Nullable
    TxnId primaryTxnId();

    void process(Node on, BiConsumer<? super R, Throwable> callback);

    interface Handler
    {
        <R> void handle(LocalRequest<R> message, BiConsumer<? super R, Throwable> callback, Node node);
    }

    static <R> void simpleHandler(LocalRequest<R> message, BiConsumer<? super R, Throwable> callback, Node node)
    {
        message.process(node, callback);
    }
}
