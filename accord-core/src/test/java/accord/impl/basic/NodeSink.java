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

package accord.impl.basic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import accord.local.AgentExecutor;
import accord.local.PreLoadContext;
import accord.messages.Message;
import accord.messages.SafeCallback;
import accord.messages.TxnRequest;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.RandomSource;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.impl.basic.Packet.SENTINEL_MESSAGE_ID;

public class NodeSink implements MessageSink
{
    private static final boolean DEBUG = true;
    // TODO (now): once CASSANDRA-18451 lands, make this private
    public enum Action {DELIVER, DROP, DROP_PARTITIONED, TIMEOUT, FAILURE}
    public enum ClientAction {SUBMIT, SUCCESS, FAILURE}
    final Id self;
    final Function<Id, Node> lookup;
    final Cluster parent;
    final RandomSource random;

    int nextMessageId = 0;
    Map<Long, SafeCallback> callbacks = new LinkedHashMap<>();

    public NodeSink(Id self, Function<Id, Node> lookup, Cluster parent, RandomSource random)
    {
        this.self = self;
        this.lookup = lookup;
        this.parent = parent;
        this.random = random;
    }

    @Override
    public synchronized void send(Id to, Request send)
    {
        debug(to, SENTINEL_MESSAGE_ID, send, Action.DELIVER);
        parent.add(self, to, SENTINEL_MESSAGE_ID, send);
    }

    @Override
    public void send(Id to, Request send, AgentExecutor executor, Callback callback)
    {
        long messageId = nextMessageId++;
        SafeCallback sc = new SafeCallback(executor, callback);
        callbacks.put(messageId, sc);
        debug(to, messageId, send, Action.DELIVER);
        parent.add(self, to, messageId, send);
        parent.pending.add((PendingRunnable) () -> {
            if (sc == callbacks.get(messageId))
                sc.slowResponse(to);
        }, 100 + random.nextInt(200), TimeUnit.MILLISECONDS);
        parent.pending.add((PendingRunnable) () -> {
            if (sc == callbacks.remove(messageId))
            {
                debug(to, messageId, send, Action.TIMEOUT);
                sc.timeout(to);
            }
        }, 1000 + random.nextInt(10000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
    {
        long id = Packet.getMessageId(replyContext);
        debug(replyToNode, id, reply, Action.DELIVER);
        parent.add(self, replyToNode, id, reply);
    }

    // TODO (now): once CASSANDRA-18451 lands, make this private
    public void debug(Id to, long id, Message message, Action action)
    {
        if (!DEBUG)
            return;
        if (Debug.txnIdFilter.isEmpty() || Debug.containsTxnId(self, to, id, message))
            Debug.logger.debug("Message {}: From {}, To {}, id {}, Message {}", Debug.normalize(action), Debug.normalize(self), Debug.normalize(to), Debug.normalizeMessageId(id), message);
    }

    public void debugClient(TxnId id, Object message, ClientAction action)
    {
        if (!DEBUG)
            return;
        if (Debug.txnIdFilter.isEmpty() || Debug.txnIdFilter.contains(id))
        {
            String log = message instanceof Throwable ? "Client  {}: From {}, To {}, id {}" : "Client  {}: From {}, To {}, id {}, Message {}";
            Debug.logger.debug(log, Debug.normalize(action), Debug.normalize(self), Debug.normalize(self), Debug.normalize(id), Debug.normalizeClientMessage(message));
        }
    }

    private static class Debug
    {
        private static final Logger logger = LoggerFactory.getLogger(Debug.class);
        // to limit logging to specific TxnId, list in the set below
        private static final Set<TxnId> txnIdFilter = ImmutableSet.of(TxnId.fromValues(3,86207,3,10));
        private static final Set<TxnReplyId> txnReplies = new HashSet<>();

        private static int ACTION_SIZE = Stream.of(Action.values()).map(Enum::name).mapToInt(String::length).max().getAsInt();
        private static int CLIENT_ACTION_SIZE = Stream.of(ClientAction.values()).map(Enum::name).mapToInt(String::length).max().getAsInt();
        private static int ALL_ACTION_SIZE = Math.max(ACTION_SIZE, CLIENT_ACTION_SIZE);

        private static Object normalizeClientMessage(Object o)
        {
            if (o instanceof Throwable)
                trimStackTrace((Throwable) o);
            return o;
        }

        private static void trimStackTrace(Throwable input)
        {
            for (Throwable current = input; current != null; current = current.getCause())
            {
                StackTraceElement[] stack = current.getStackTrace();
                // remove junit as its super dense and not helpful
                OptionalInt first = IntStream.range(0, stack.length).filter(i -> stack[i].getClassName().startsWith("org.junit")).findFirst();
                if (first.isPresent())
                    current.setStackTrace(Arrays.copyOfRange(stack, 0, first.getAsInt()));
                for (Throwable sup : current.getSuppressed())
                    trimStackTrace(sup);
            }
        }

        private static String normalize(Action action)
        {
            return Strings.padStart(action.name(), ALL_ACTION_SIZE, ' ');
        }

        private static String normalize(ClientAction action)
        {
            return Strings.padStart(action.name(), ALL_ACTION_SIZE, ' ');
        }

        private static String normalize(Id id)
        {
            return Strings.padStart(id.toString(), 4, ' ');
        }

        private static String normalizeMessageId(long id)
        {
            return Strings.padStart(Long.toString(id), 14, ' ');
        }

        private static String normalize(Timestamp ts)
        {
            return Strings.padStart(ts.toString(), 14, ' ');
        }

        public static boolean containsTxnId(Node.Id from, Node.Id to, long id, Message message)
        {
            if (message instanceof Request)
            {
                if (containsAny((Request) message))
                {
                    txnReplies.add(new TxnReplyId(from, to, id));
                    return true;
                }
                return false;
            }
            else
                return txnReplies.contains(new TxnReplyId(to, from, id));
        }

        private static boolean containsAny(Request message)
        {
            if (message instanceof TxnRequest<?>)
                return txnIdFilter.contains(((TxnRequest<?>) message).txnId);
            // this includes txn that depend on the txn, should this limit for the first txnId?
            if (message instanceof PreLoadContext)
            {
                PreLoadContext context = (PreLoadContext) message;
                if (context.primaryTxnId() != null && txnIdFilter.contains(context.primaryTxnId()))
                    return true;
                return context.additionalTxnIds().stream().anyMatch(txnIdFilter::contains);
            }
            return false;
        }

        private static class TxnReplyId
        {
            final Id from;
            final Id to;
            final long id;

            private TxnReplyId(Id from, Id to, long id)
            {
                this.from = from;
                this.to = to;
                this.id = id;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TxnReplyId that = (TxnReplyId) o;
                return id == that.id && Objects.equals(from, that.from) && Objects.equals(to, that.to);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(from, to, id);
            }

            @Override
            public String toString()
            {
                return "TxnReplyId{" +
                       "from=" + from +
                       ", to=" + to +
                       ", id=" + id +
                       '}';
            }
        }
    }
}
