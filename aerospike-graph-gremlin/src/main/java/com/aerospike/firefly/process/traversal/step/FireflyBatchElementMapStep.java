package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FireflyBatchElementMapStep extends CollectingBarrierStep<Element> implements LocalBarrier<Element> {
    private final int barrierSize;
    private final String[] propertyKeys;

    public FireflyBatchElementMapStep(final Traversal.Admin traversal,
                                      final Set<String> labels,
                                      final int barrierSize,
                                      final String... propertyKeys) {
        super(traversal, barrierSize);
        this.labels = new HashSet<>(labels);
        this.barrierSize = barrierSize;
        this.propertyKeys = propertyKeys;
    }

    @Override
    public void barrierConsumer(final TraverserSet<Element> set) {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);

        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet output = new TraverserSet<>();
        // pair contains vertex id and label
        final Map<FireflyId, Pair<Object, String>> vertexCache = getVertexCache(set, graph);

        while (!set.isEmpty()) {
            // copy paste from ElementStep
            final Traverser.Admin traverser = set.remove();
            final Element element = (Element) traverser.get();

            final Map<Object, Object> map = new LinkedHashMap<>();
            map.put(T.id, element.id());
            if (element instanceof VertexProperty) {
                map.put(T.key, ((VertexProperty<?>) element).key());
                map.put(T.value, ((VertexProperty<?>) element).value());
            } else {
                map.put(T.label, element.label());
            }

            if (element instanceof FireflyEdge) {
                final FireflyEdge e = (FireflyEdge) element;
                // only this block is different from original ElementStep
                map.put(Direction.IN, getVertexStructure(e.inVertexId(), vertexCache));
                map.put(Direction.OUT, getVertexStructure(e.outVertexId(), vertexCache));
            }

            final Iterator<? extends Property> properties = element.properties(this.propertyKeys);
            while (properties.hasNext()) {
                final Property<?> property = properties.next();
                map.put(property.key(), property.value());
            }

            output.add(traverser.split(map, this));
        }

        if (output.isEmpty()) {
            set.add(EmptyTraverser.instance());
        } else {
            set.clear();
            set.addAll(output);
            output.clear(); // Force garbage collection.
        }
    }

    protected Map<Object, Object> getVertexStructure(final FireflyId vertexId, final Map<FireflyId, Pair<Object, String>> vertexCache) {
        final Map<Object, Object> m = new LinkedHashMap<>();

        if (vertexCache.containsKey(vertexId)) {
            m.put(T.id, vertexCache.get(vertexId).getValue0());
            m.put(T.label, vertexCache.get(vertexId).getValue1());
        } else {
            // Vertex was remoted or Edge is stray
            m.put(T.id, vertexId.getUserId());
        }

        return m;
    }

    private Map<FireflyId, Pair<Object, String>> getVertexCache(final TraverserSet<Element> set, final FireflyGraph graph) {
        final List<FireflyId> vertexIdsToRead = new ArrayList<>();

        final Map<FireflyId, Pair<Object, String>> cache = new HashMap<>();
        for (final Traverser<Element> traverser : set) {
            if (traverser.get() instanceof FireflyEdge) {
                final FireflyEdge edge = (FireflyEdge) traverser.get();
                if (!cache.containsKey(edge.outVertexId())) {
                    vertexIdsToRead.add(edge.outVertexId());
                }
                if (!cache.containsKey(edge.inVertexId())) {
                    vertexIdsToRead.add(edge.inVertexId());
                }

                if (vertexIdsToRead.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                    graph.readVertices(Collections.emptyList(), vertexIdsToRead, Collections.emptyList(), false)
                            .forEach(v -> cache.put(v.id, Pair.with(v.id(), v.label())));
                    vertexIdsToRead.clear();
                }
            }
        }

        graph.readVertices(Collections.emptyList(), vertexIdsToRead, Collections.emptyList(), false)
                .forEach(v -> cache.put(v.id, Pair.with(v.id(), v.label())));
        vertexIdsToRead.clear();

        return cache;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.barrierSize, Arrays.asList(this.propertyKeys));
    }
}
