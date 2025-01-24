package com.aerospike.firefly.olap.structure;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedMessenger<M> implements Messenger<M> {

    private Vertex vertex;
    private Iterable<M> incomingMessages;
    private List<Tuple2<Object, M>> outgoingMessages = new ArrayList<>();

    public void setVertexAndIncomingMessages(final Vertex vertex, final Iterable<M> incomingMessages) {
        this.vertex = vertex;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = new ArrayList<>();
    }

    public List<Tuple2<Object, M>> getOutgoingMessages() {
        return this.outgoingMessages;
    }

    public List<Vertex> getVerticesWithActiveTraversers() {
        return null;
    }

    @Override
    public Iterator<M> receiveMessages() {
        if (incomingMessages != null && incomingMessages.iterator().hasNext()) {
            return IteratorUtils.removeOnNext(this.incomingMessages.iterator());
        } else {
            return EmptyIterator.instance();
        }
    }

    @Override
    public void sendMessage(final MessageScope messageScope, final M message) {
        System.out.println("!!!!SEND");
        if (messageScope instanceof MessageScope.Local) {
            final MessageScope.Local<M> localMessageScope = (MessageScope.Local) messageScope;
            final Traversal.Admin<Vertex, Edge> incidentTraversal = DistributedMessenger.setVertexStart(localMessageScope.getIncidentTraversal().get().asAdmin(), this.vertex);
            final Direction direction = DistributedMessenger.getOppositeDirection(incidentTraversal);

            // handle processing for BOTH given TINKERPOP-1862 where the target of the message is the one opposite
            // the current vertex
            incidentTraversal.forEachRemaining(edge -> {
                if (direction.equals(Direction.IN) || direction.equals(Direction.OUT))
                    this.outgoingMessages.add(new Tuple2<>(edge.vertices(direction).next().id(), localMessageScope.getEdgeFunction().apply(message, edge)));
                else
                    this.outgoingMessages.add(new Tuple2<>(edge instanceof StarGraph.StarOutEdge ? edge.inVertex().id() : edge.outVertex().id(), localMessageScope.getEdgeFunction().apply(message, edge)));

            });
        } else {
            ((MessageScope.Global) messageScope).vertices().forEach(v -> this.outgoingMessages.add(new Tuple2<>(v.id(), message)));
        }
    }

    ///////////

    private static <T extends Traversal.Admin<Vertex, Edge>> T setVertexStart(final Traversal.Admin<Vertex, Edge> incidentTraversal, final Vertex vertex) {
        incidentTraversal.asAdmin().addStart(incidentTraversal.getTraverserGenerator().generate(vertex, incidentTraversal.asAdmin().getStartStep(), 1l));
        return (T) incidentTraversal;
    }

    private static Direction getOppositeDirection(final Traversal.Admin<Vertex, Edge> incidentTraversal) {
        final VertexStep step = TraversalHelper.getLastStepOfAssignableClass(VertexStep.class, incidentTraversal).get();
        return step.getDirection().opposite();
    }

    public List<M> getVerticesWithActiveTraversersIncoming() {
        final List<M> result = new ArrayList<>();
        incomingMessages.forEach(itty -> result.add(itty));
        return result;
    }
}
