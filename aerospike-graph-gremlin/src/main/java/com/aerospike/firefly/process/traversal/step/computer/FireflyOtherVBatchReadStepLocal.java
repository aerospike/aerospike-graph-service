package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FireflyOtherVBatchReadStepLocal extends FlatMapStep<Edge, Vertex> {
    private final List<HasContainer> fireflyHasContainers;
    private final List<HasContainer> aerospikeHasContainers;

    private transient final Map<FireflyId, FireflyVertex> cache = new HashMap<>();
    private transient final List<FireflyId> inputCache =new ArrayList<>();
    private final boolean areEdgesRequired;
    private boolean first = true;

    public FireflyOtherVBatchReadStepLocal(final Traversal.Admin traversal,
                                           final List<HasContainer> hasContainers,
                                           final Set<String> labels,
                                           final boolean areEdgesRequired) {
        super(traversal);
        this.areEdgesRequired = areEdgesRequired;
        this.labels = new HashSet<>(labels);
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
    public void addStart(final Traverser.Admin<Edge> start) {
        super.addStart(start);
        add(start);
        first = true;
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<Edge> traverser) {
        if (!traversal.isRoot() && !(traversal.getParent() instanceof TraversalVertexProgramStep)) {
            add(traverser);
            precompute();
        } else if (first) {
            // TODO: Be smarter b/c we should keep some of this if there's intersections.
            cache.clear();
            precompute();
            first = false;
        }

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
                Vertex outVertex = cache.get(edge.outVertexId()), inVertex = cache.get(edge.inVertexId());
                final Vertex result;
                if (outVertex == null && inVertex == null) {
                    // cache might be outdated. Or precompute skipped
                    outVertex = traverser.get().outVertex();
                    result = ElementHelper.areEqual((Vertex) objects.get(i), outVertex) ? traverser.get().inVertex() : outVertex;
                } else {
                    result = outVertex != null && !ElementHelper.areEqual(vertex, outVertex) ? outVertex : inVertex;
                }
                if (HasContainer.testAll(result, fireflyHasContainers))
                    return FireflyCloseableIteratorUtils.of(result);
                return Collections.emptyIterator();
            }
        }
        throw new IllegalStateException("The path history of the traverser does not contain a previous vertex: " + traverser.path());
    }

    private void precompute() {
        if (inputCache.isEmpty()) return;
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        final List<FireflyId> chunk = new ArrayList<>();
        for (final FireflyId id : inputCache) {
            chunk.add(id);
            if (chunk.size() == graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                final List<FireflyVertex> vertices = graph.readVertices(aerospikeHasContainers, chunk, null, areEdgesRequired);
                for (final FireflyVertex vertex : vertices) {
                    cache.put(vertex.id, vertex);
                }
                chunk.clear();
            }
        }
        if (!chunk.isEmpty()) {
            final List<FireflyVertex> vertices = graph.readVertices(aerospikeHasContainers, chunk, null, areEdgesRequired);
            for (final FireflyVertex vertex : vertices) {
                cache.put(vertex.id, vertex);
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
                inputCache.add(graph.getIdFactory().createVertexId(vertex));
                return;
            }
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }
}
