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

import accord.api.MessageSink;
import accord.coordinate.Timeout;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.SafeCallback;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class SimpleSinks
{
    private final Map<Node.Id, Node> nodes = new HashMap<>();
    private final Map<Node.Id, Set<Node.Id>> outboundPartition = new ConcurrentHashMap<>();
    private final Map<Node.Id, Set<Predicate<Request>>> outboundRequestFilters = new ConcurrentHashMap<>();
    private final Map<Node.Id, Set<ReplyPredicate>> outboundReplyFilters = new ConcurrentHashMap<>();

    public void register(Node... ns)
    {
        for (Node n : ns)
        {
            assert !nodes.containsKey(n.id()): String.format("Node %s already exists", n);
            nodes.put(n.id(), n);
        }
    }
    public MessageSink sinkFor(Node.Id id)
    {
        return new Sink(id);
    }

    public MessageSink mockedSinkFor(MockType type, Node.Id id)
    {
        MessageSink sink = sinkFor(id);
        switch (type)
        {
            case NO_OP:
                return Mockito.mock(Sink.class, Mockito.withSettings().spiedInstance(sink));
            case CALL_REAL:
                return Mockito.spy(sink);
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    private Node node(Node.Id id)
    {
        assert nodes.containsKey(id): String.format("Node %s does not exist", id);
        return nodes.get(id);
    }

    public void partitionOutbound(Node.Id src, Node.Id dst)
    {
        outboundPartition.computeIfAbsent(src, ignore -> new CopyOnWriteArraySet<>()).add(dst);
    }

    public void removePartitionOutbound(Node.Id src, Node.Id dst)
    {
        if (!outboundPartition.containsKey(src)) return;
        outboundPartition.computeIfAbsent(src, ignore -> new CopyOnWriteArraySet<>()).remove(dst);
    }

    public Releaser outboundFilter(Predicate<Request> fn)
    {
        for (Node.Id id : nodes.keySet())
            outboundRequestFilters.computeIfAbsent(id, ignore -> new CopyOnWriteArraySet<>()).add(fn);
        return () -> {
            for (Node.Id id : nodes.keySet())
                outboundRequestFilters.getOrDefault(id, Collections.emptySet()).remove(fn);
        };
    }

    public Releaser replyOrdering(Node.Id to, Class<? extends Reply> replyType, Node.Id... order)
    {
        OrderingFilter filter = new OrderingFilter(to, replyType, Arrays.asList(order));
        for (Node.Id id : order)
            outboundReplyFilters.computeIfAbsent(id, ignore -> new CopyOnWriteArraySet<>()).add(filter);
        return filter;
    }

    private static class ReplyWithContext
    {
        private final ReplyContext context;
        private final Reply reply;

        private ReplyWithContext(ReplyContext context, Reply reply)
        {
            this.context = context;
            this.reply = reply;
        }
    }

    private class OrderingFilter implements ReplyPredicate, Releaser
    {
        private final Node.Id to;
        private final Class<? extends Reply> replyType;
        private final List<Node.Id> order;
        private final Map<Node.Id, ReplyWithContext> replies = new HashMap<>();

        private OrderingFilter(Node.Id to, Class<? extends Reply> replyType, List<Node.Id> order)
        {
            this.to = to;
            this.replyType = replyType;
            this.order = order;
        }

        @Override
        public synchronized boolean test(Node.Id src, Node.Id dest, ReplyContext context, Reply reply)
        {
            if (!replyType.isAssignableFrom(reply.getClass()))
                return true;
            if (!order.contains(src))
                throw new IllegalStateException("Unexpected reply from " + src + "; expected " + order);
            replies.put(src, new ReplyWithContext(context, reply));
            if (replies.size() == order.size())
            {
                // done
                close();
                for (Node.Id id : order)
                {
                    MessageSink sink = node(id).messageSink();
                    ReplyWithContext value = replies.get(id);
                    sink.reply(dest, value.context, value.reply);
                }
            }
            return false;
        }

        @Override
        public void close()
        {
            for (Node.Id id : order)
                outboundReplyFilters.getOrDefault(id, Collections.emptySet()).remove(this);
        }
    }

    public enum MockType { CALL_REAL, NO_OP }

    public interface Releaser extends AutoCloseable
    {
        @Override
        void close();
    }

    public interface ReplyPredicate
    {
        boolean test(Node.Id src, Node.Id to, ReplyContext context, Reply reply);
    }

    private class Sink implements MessageSink
    {
        private final Node.Id src;
        private final AtomicLong msgIds = new AtomicLong();
        private final Map<Long, SafeCallback> callbacks = new ConcurrentHashMap<>();

        public Sink(Node.Id src)
        {
            this.src = src;
        }

        private boolean maySend(Node.Id to, Request request)
        {
            if (outboundPartition.getOrDefault(src, Collections.emptySet()).contains(to)) return false;
            for (Predicate<Request> filter : outboundRequestFilters.getOrDefault(src, Collections.emptySet()))
            {
                if (!filter.test(request)) return false;
            }
            return true;
        }

        private boolean maySend(Node.Id to, ReplyContext replyContext, Reply reply)
        {
            if (outboundPartition.getOrDefault(src, Collections.emptySet()).contains(to)) return false;
            for (ReplyPredicate filter : outboundReplyFilters.getOrDefault(src, Collections.emptySet()))
            {
                if (!filter.test(src, to, replyContext, reply)) return false;
            }
            return true;
        }

        @Override
        public void send(Node.Id to, Request request)
        {
            if (!maySend(to, request)) return;
            node(to).receive(request, src, NoReply.INSTANCE);
        }

        @Override
        public void send(Node.Id to, Request request, AgentExecutor executor, Callback callback)
        {
            if (!maySend(to, request))
            {
                executor.execute(() -> callback.onFailure(to, new Timeout(null, null)));
                return;
            }
            Node dst = node(to); // fetch early to make sure no local state is updated unless node exists
            long id = msgIds.incrementAndGet();
            callbacks.put(id, new SafeCallback(executor, callback));
            dst.receive(request, src, new Context(id));
        }

        @Override
        public void reply(Node.Id replyingToNode, ReplyContext replyContext, Reply reply)
        {
            if (!maySend(replyingToNode, replyContext, reply)) return;
            if (replyContext instanceof Context)
                ((Context) replyContext).reply(src, replyingToNode, reply);
        }

        private class Context implements ReplyContext
        {
            private final long id;

            private Context(long id)
            {
                this.id = id;
            }

            private void reply(Node.Id from, Node.Id replyingToNode, Reply reply)
            {
                assert src.equals(replyingToNode): String.format("Wrong node replied to: expected %s but given %s", src, replyingToNode);
                SafeCallback callback = reply.isFinal() ? callbacks.remove(id) : callbacks.get(id);
                if (callback != null)
                    callback.success(from, reply);
            }
        }
    }

    private enum NoReply implements ReplyContext
    {
        INSTANCE;
    }
}
