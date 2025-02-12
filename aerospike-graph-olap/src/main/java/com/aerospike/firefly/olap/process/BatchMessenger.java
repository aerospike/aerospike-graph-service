package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.process.computer.local.LocalMessageBoard;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BatchMessenger<M> implements Messenger<M> {

    private final LocalMessageBoard<M> messageBoard;
    private final MessageCombiner<M> combiner;

    public BatchMessenger(final LocalMessageBoard<M> messageBoard, final Optional<MessageCombiner<M>> combiner) {
        this.messageBoard = messageBoard;
        this.combiner = combiner.isPresent() ? combiner.get() : null;
    }

    public Iterator<M> receiveMessages() {
        throw new IllegalStateException("should never be called");
    }

    public void sendMessage(final MessageScope messageScope, final M message) {
        if (messageScope instanceof MessageScope.Local) {
            throw new IllegalStateException("Local messages not supported on DistributedGraphComputer.");
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> addMessage(v, message, messageScope));
        }
    }

    private void addMessage(final Vertex vertex, final M message, MessageScope messageScope) {
        this.messageBoard.sendMessages.compute(messageScope, (ms, messages) -> {
            if (null == messages) messages = new ConcurrentHashMap<>();
            return messages;
        });

        this.messageBoard.sendMessages.get(messageScope).compute(vertex, (v, queue) -> {
            if (null == queue) queue = new ConcurrentLinkedQueue<>();
            queue.add(null != this.combiner && !queue.isEmpty() ? this.combiner.combine(queue.remove(), message) : message);
            return queue;
        });
    }
}
