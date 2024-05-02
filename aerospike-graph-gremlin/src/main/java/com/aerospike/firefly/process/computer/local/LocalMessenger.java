package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.MultiIterator;

import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalMessenger<M> implements Messenger<M> {

    private final Vertex vertex;
    private final LocalMessageBoard<M> messageBoard;
    private final MessageCombiner<M> combiner;

    public LocalMessenger(final Vertex vertex, final LocalMessageBoard<M> messageBoard, final Optional<MessageCombiner<M>> combiner) {
        this.vertex = vertex;
        this.messageBoard = messageBoard;
        this.combiner = combiner.isPresent() ? combiner.get() : null;
    }

    @Override
    public Iterator<M> receiveMessages() {
        final MultiIterator<M> multiIterator = new MultiIterator<>();
        for (final MessageScope messageScope : this.messageBoard.receiveMessages.keySet()) {
//        for (final MessageScope messageScope : this.messageBoard.previousMessageScopes) {
            if (messageScope instanceof MessageScope.Local) {
                final MessageScope.Local<M> localMessageScope = (MessageScope.Local<M>) messageScope;
                final Traversal.Admin<Vertex, Edge> incidentTraversal = LocalMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get().asAdmin(), this.vertex);
                final Direction direction = LocalMessenger.getDirection(incidentTraversal);
                final Edge[] edge = new Edge[1]; // simulates storage side-effects available in Gremlin, but not Java streams
                multiIterator.addIterator(StreamSupport.stream(Spliterators.spliteratorUnknownSize(VertexProgramHelper.reverse(incidentTraversal.asAdmin()), Spliterator.IMMUTABLE | Spliterator.SIZED), false)
                        .map((Edge e) -> {
                            edge[0] = e;
                            Vertex vv;
                            if (direction.equals(Direction.IN) || direction.equals(Direction.OUT)) {
                                vv = e.vertices(direction).next();
                            } else {
                                vv = e.outVertex() == this.vertex ? e.inVertex() : e.outVertex();
                            }
                            return this.messageBoard.receiveMessages.get(messageScope).get(vv);
                        })
                        .filter(q -> null != q)
                        .flatMap(Queue::stream)
                        .map(message -> localMessageScope.getEdgeFunction().apply(message, edge[0]))
                        .iterator());

            } else {
                multiIterator.addIterator(Stream.of(this.vertex)
                        .map(this.messageBoard.receiveMessages.get(messageScope)::get)
                        .filter(q -> null != q)
                        .flatMap(Queue::stream)
                        .iterator());
            }
        }
        return multiIterator;
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
//        this.messageBoard.currentMessageScopes.add(messageScope);
        if (messageScope instanceof MessageScope.Local) {
            addMessage(this.vertex, message, messageScope);
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> addMessage(v, message, messageScope));
        }
    }

    private void addMessage(final Vertex vertex, final M message, MessageScope messageScope) {
        this.messageBoard.sendMessages.compute(messageScope, (ms, messages) -> {
            if(null==messages) messages = new ConcurrentHashMap<>();
            return messages;
        });

        this.messageBoard.sendMessages.get(messageScope).compute(vertex, (v, queue) -> {
            if (null == queue) queue = new ConcurrentLinkedQueue<>();
            queue.add(null != this.combiner && !queue.isEmpty() ? this.combiner.combine(queue.remove(), message) : message);
            return queue;
        });
    }

    ///////////

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(final Traversal.Admin<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.addStart(incidentTraversal.getTraverserGenerator().generate(vertex,incidentTraversal.getStartStep(), 1L));
        return (T) incidentTraversal;
    }

    private static Direction getDirection(final Traversal.Admin<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
        return step.getDirection();

    }
}