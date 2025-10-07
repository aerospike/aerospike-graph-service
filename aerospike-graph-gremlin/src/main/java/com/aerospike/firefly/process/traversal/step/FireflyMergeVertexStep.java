package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.CardinalityValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventUtil;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep.searchVerticesOptimally;
import static com.aerospike.firefly.structure.FireflyGraph.BULK_LOAD_VERTEX_ADD_KEY;

public class FireflyMergeVertexStep<S> extends MergeVertexStep<S> {
    long evaluationTimeout;

    public FireflyMergeVertexStep(final MergeVertexStep step) {
        this(step.getTraversal(), step.isStart(), step.getMergeTraversal());
        if (step.getOnMatchTraversal() != null) this.addChildOption(Merge.onMatch, step.getOnMatchTraversal());
        if (step.getOnCreateTraversal() != null) this.addChildOption(Merge.onCreate, step.getOnCreateTraversal());
        if (step.getCallbackRegistry() != null) this.callbackRegistry = step.getCallbackRegistry();
        step.getLabels().forEach(label -> addLabel((String) label));
        this.evaluationTimeout = TimeoutHelper.calculate(step.getTraversal());
    }

    public FireflyMergeVertexStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal());
    }

    public FireflyMergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Map merge) {
        this(traversal, isStart, new ConstantTraversal<>(merge));
    }

    public FireflyMergeVertexStep(final Traversal.Admin traversal, final boolean isStart,
                                  final Traversal.Admin<S, Map> mergeTraversal) {
        super(traversal, isStart, mergeTraversal.asAdmin());
    }

    @Override
    protected CloseableIterator<Vertex> searchVertices(final Map search) {
        if (search == null) {
            return CloseableIterator.empty();
        }

        final FireflyGraph graph = (FireflyGraph) getGraph();
        final List<String> requiredProperties = new ArrayList<>();
        final List<HasContainer> hasContainers = new ArrayList<>();
        if (search.containsKey(T.label)) {
            final HasContainer labelHasContainer = new HasContainer(T.label.getAccessor(), P.eq(search.get(T.label)));
            hasContainers.add(labelHasContainer);
        }

        for (final Object entry : search.entrySet()) {
            final Map.Entry<Object, Object> searchPair = (Map.Entry<Object, Object>) entry;
            if (searchPair.getKey() instanceof String) {
                final String stringKey = (String) searchPair.getKey();
                requiredProperties.add(stringKey);
                final HasContainer propertyHasContainer = new HasContainer(stringKey, P.eq(searchPair.getValue()));
                hasContainers.add(propertyHasContainer);
            }
        }

        // Box in array here since Vertex search function can batch read multiple IDs.
        final Object[] id = search.containsKey(T.id) ? new Object[]{search.get(T.id)} : new Object[0];

        final Iterator<Vertex> vertexSearch = searchVerticesOptimally(graph, hasContainers, requiredProperties,
                this.evaluationTimeout, id);
        return CloseableIterator.of(vertexSearch);
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<S> traverser) {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        final Map mergeMap = materializeMap(traverser, mergeTraversal);
        validateMapInput(mergeMap, false);

        if (onMatchTraversal instanceof ConstantTraversal) {
            final Map matchMap = onMatchTraversal.next();
            validateMapInput(matchMap, true);
        }

        final Map<?, ?> onCreateMap = onCreateMap(traverser, mergeMap);

        Iterator<Vertex> vertices = searchVertices(mergeMap);
        while (true) {
            try {
                if (onMatchTraversal != null && vertices.hasNext()) {
                    vertices = IteratorUtils.map(vertices, v -> {
                        // override current traverser with the matched Vertex so that the option() traversal can operate
                        // on it properly. this should only work this way for the start step form to retain the original
                        // behavior for 3.6.0 where you might do g.inject(Map).mergeV() and want that Map to pass through.
                        // in 4.x this will be rectified such that the vertex will always be promoted and you will be forced
                        // to select() the map if you did want the behavior.
                        if (isStart) traverser.set((S) v);

                        // assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
                        final Map<String, Object> onMatchMap = materializeMap(traverser, onMatchTraversal);
                        validateMapInput(onMatchMap, true);

                        final List<VertexProperty.Cardinality> cardinalities = new ArrayList<>();
                        final List<String> keys = new ArrayList<>();
                        final List<Object> values = new ArrayList<>();
                        onMatchMap.forEach((key, value) -> {
                            Object val = value;
                            VertexProperty.Cardinality card = graph.features().vertex().getCardinality(key);

                            // a value can be a traversal in the case where the user specifies the cardinality for the value.
                            if (value instanceof CardinalityValueTraversal) {
                                final CardinalityValueTraversal cardinalityValueTraversal = (CardinalityValueTraversal) value;
                                card = cardinalityValueTraversal.getCardinality();
                                val = cardinalityValueTraversal.getValue();
                            }

                            // trigger callbacks for eventing - in this case, it's a VertexPropertyChangedEvent. if there's no
                            // registry/callbacks then just set the property
                            EventUtil.registerVertexPropertyChange(callbackRegistry, getTraversal(), v, key, val);

                            cardinalities.add(card);
                            keys.add(key);
                            values.add(val);
                        });

                        ((FireflyVertex) v).batchWriteMergeVProperties(cardinalities, keys, values);
                        if (onCreateMap.containsKey(BULK_LOAD_VERTEX_ADD_KEY)) {
                            // Do not need properties so leave blank.
                            graph.fireflySummaryUpdater.stageVertexMergeToQueue(v.label(), (Integer) onCreateMap.get(BULK_LOAD_VERTEX_ADD_KEY));
                        }
                        return v;
                    });
                }
                vertices = IteratorUtils.filter(vertices, Objects::nonNull);

                if (vertices.hasNext()) {
                    // onMatch
                    return vertices;
                } else {
                    // make sure we close the search traversal
                    CloseableIterator.closeIterator(vertices);
                }

                // onCreate
                // This logic in the TinkerPop MergeVertexStep is wack. They separate out properties that are specified
                // with a cardinality but the onCreateMap can only have unique keys so the cardinality doesn't even
                // matter. We can use custom logic to write all the properties when creating the Vertex.
                final Object[] flatArgs = onCreateMap.entrySet().stream().flatMap(e -> {
                    final Object value;
                    if (e.getValue() instanceof CardinalityValueTraversal) {
                        value = ((CardinalityValueTraversal) e.getValue()).getValue();
                    } else {
                        value = e.getValue();
                    }
                    return Stream.of(e.getKey(), value);
                }).toArray();

                final Vertex vertex = graph.addVertex(flatArgs);

                // trigger callbacks for eventing - in this case, it's a VertexAddedEvent
                EventUtil.registerVertexCreationWithGenericEventRegistry(callbackRegistry, getTraversal(), vertex);

                return IteratorUtils.of(vertex);
            } catch (final IllegalArgumentException e) {
                if (e.getMessage().contains("Vertex with id already exists:")) {
                    // T.id will always exist in the onCreateMap if we get this exception case
                    final Iterator<Vertex> vertexWithId = graph.vertices(onCreateMap.get(T.id));
                    vertices = searchVertices(mergeMap);
                    // If the search can't find any matches after failing to create a Vertex with the id, don't retry
                    // to prevent an infinite loop.
                    if (!vertices.hasNext() && vertexWithId.hasNext()) {
                        CloseableIterator.closeIterator(vertexWithId);
                        CloseableIterator.closeIterator(vertices);
                        throw e;
                    }
                    CloseableIterator.closeIterator(vertexWithId);
                } else {
                    throw e;
                }
            }
        }
    }
}
