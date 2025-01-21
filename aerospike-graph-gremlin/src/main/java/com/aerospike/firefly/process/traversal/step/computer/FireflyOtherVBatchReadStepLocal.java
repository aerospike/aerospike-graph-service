package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FireflyOtherVBatchReadStepLocal extends AbstractStep<Edge, Vertex> {
    private final List<HasContainer> fireflyHasContainers;
    private final List<HasContainer> aerospikeHasContainers;
    private final List<String> requiredProperties;
    private static final ThreadLocal<Map<FireflyId, FireflyVertex>> cache =
            ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<Set<FireflyId>> inputCache =
            ThreadLocal.withInitial(HashSet::new);

    public FireflyOtherVBatchReadStepLocal(final Traversal.Admin traversal,
                                           final List<HasContainer> hasContainers,
                                           final Set<String> labels,
                                           final List<String> requiredProperties) {
        super(traversal);
        this.labels = new HashSet<>(labels);
        this.requiredProperties = requiredProperties;
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) traversal.getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        System.out.println("FireflyOtherVBatchReadStepLocal.processNextStart");
        while (true) {
            final Traverser.Admin<Edge> traverser = this.starts.next();
            final Vertex result = handle(traverser);
            if (result != null)
                return traverser.split(result, this);
        }
    }

    private Vertex handle(final Traverser.Admin<Edge> traverser) {
        List<Object> objects = traverser.path().objects();
        for (int i = objects.size() - 2; i >= 0; --i) {
            if (objects.get(i) instanceof Vertex) {
                // vertex is Reference, so don't have FireflyId
                final Vertex vertex = (Vertex) objects.get(i);
                // FireflyEdge have only FireflyId of In/Out vertices
                final FireflyEdge edge = (FireflyEdge) (traverser.get() instanceof ComputerGraph.ComputerEdge
                        ? ((ComputerGraph.ComputerEdge) traverser.get()).getBaseEdge()
                        : traverser.get());
                // try to get something without reading from DB
                // at least one should be not empty
                Vertex outVertex = cache.get().get(edge.outVertexId()), inVertex = cache.get().get(edge.inVertexId());
                final Vertex result;
                if (outVertex == null && inVertex == null) {
                    // cache might be outdated. Or precompute skipped
                    outVertex = traverser.get().outVertex();
                    result = ElementHelper.areEqual((Vertex) objects.get(i), outVertex) ? traverser.get().inVertex() : outVertex;
                } else {
                    result = outVertex != null && !ElementHelper.areEqual(vertex, outVertex) ? outVertex : inVertex;
                }
                if (HasContainer.testAll(result, fireflyHasContainers))
                    return result;
                return null;
            }
        }
        throw new IllegalStateException("The path history of the traverser does not contain a previous vertex: " + traverser.path());
    }

    private void precompute() {
        if (inputCache.get().isEmpty()) return;
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        final List<FireflyId> chunk = new ArrayList<>();
        for (final FireflyId id : inputCache.get()) {
            chunk.add(id);
            if (chunk.size() == graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                final List<FireflyVertex> vertices = graph.readVertices(aerospikeHasContainers, chunk, requiredProperties);
                for (final FireflyVertex vertex : vertices) {
                    cache.get().put(vertex.id, vertex);
                }
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) {
            final List<FireflyVertex> vertices = graph.readVertices(aerospikeHasContainers, chunk, requiredProperties);
            for (final FireflyVertex vertex : vertices) {
                cache.get().put(vertex.id, vertex);
            }
        }
    }

    private void add(final Traverser.Admin<?> tv) {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        final List<Object> objects = tv.path().objects();
        for (int i = objects.size() - 2; i >= 0; i--) {
            if (objects.get(i) instanceof Vertex) {
                final Edge edge = (Edge) tv.get();
                final Vertex vertex = ElementHelper.areEqual((Vertex) objects.get(i), edge.outVertex())
                        ? edge.inVertex()
                        : edge.outVertex();
                inputCache.get().add(graph.getIdFactory().createVertexId(vertex));
                return;
            }
        }
    }

    public void addStart(final Traverser.Admin<Edge> start) {
        System.out.println("FireflyOtherVBatchReadStepLocal.addStart: " + start);
        this.starts.add(start);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }
}
