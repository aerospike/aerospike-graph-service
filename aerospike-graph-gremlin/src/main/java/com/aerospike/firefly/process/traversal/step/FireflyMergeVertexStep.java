package com.aerospike.firefly.process.traversal.step;

import com.aerospike.client.Key;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * adapted from MergeVertexStep
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 * {@link PartitionStrategy} is not supported.
 */
public class FireflyMergeVertexStep<S> extends MergeVertexStep<S> implements Mutating<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyMergeVertexStep.class);

    protected CallbackRegistry<Event> callbackRegistry;
    protected Long evaluationTimeout;

    public FireflyMergeVertexStep(final MergeVertexStep step) {
        this(step.getTraversal(), step.isStart(), step.getMergeTraversal());
        if (step.getOnMatchTraversal() != null) this.addChildOption(Merge.onMatch, step.getOnMatchTraversal());
        if (step.getOnCreateTraversal() != null) this.addChildOption(Merge.onCreate, step.getOnCreateTraversal());
        if (step.getCallbackRegistry() != null) this.callbackRegistry = step.getCallbackRegistry();
        this.evaluationTimeout = TimeoutHelper.calculate(step.getTraversal());
    }

    public FireflyMergeVertexStep(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal());
    }

    public FireflyMergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Map merge) {
        this(traversal, isStart, new ConstantTraversal<>(merge));
    }

    public FireflyMergeVertexStep(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<S, Map> mergeTraversal) {
        super(traversal, isStart, mergeTraversal.asAdmin());
    }

    @Override
    public void configure(final Object... keyValues) {
        // This is a Mutating step but property() should not be folded into this step.  The main issue here is that
        // this method won't know what step called it - property() or with() or something else so it can't make the
        // choice easily to throw an exception, write the keys/values to parameters, etc. It really is up to the
        // caller to make sure it is handled properly at this point. this may best be left as a do-nothing method for
        // now.
    }

    @Override
    public Parameters getParameters() {
        // Merge doesn't take fold ups of property() calls. those need to get treated as regular old PropertyStep
        // instances. not sure if this should support with() though.....none of the other Mutating steps do.
        return null;
    }

    /**
     * Use the {@code Map} of search criteria to most efficiently return a {@code Stream<Vertex>} of matching elements.
     * Providers might override this method when extending this step to provide their own optimized mechanisms for
     * matching the list of vertices. This implementation is only optimized for the {@link T#id} so any other usage
     * will simply be in-memory filtering which could be slow.
     */
    private Stream<Vertex> createSearchStream(final Map<Object, Object> search) {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        Stream<Vertex> stream;
        // Prioritize lookup by id but otherwise attempt an index lookup
        if (null == search) {
            return Stream.empty();
        } else if (search.containsKey(T.id)) {
            final Object sid = search.get(T.id);
            final FireflyId fid = graph.getIdFactory().createVertexId(sid);
            final Key askey = FireflyRecord.getKey(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, fid);
            if (graph.getBaseGraph().exists(askey)) {
                stream = FireflyCloseableIteratorUtils.stream(graph.vertices(search.get(T.id)));
            }  else {
                stream = Stream.empty();
            }
        } else {
            List<Iterator<? extends Vertex>> results = new ArrayList<>();
            search.forEach((key, value) -> {
                if (key == T.label) {
                    if (value.getClass().isAssignableFrom(Long.class) || value.getClass().isAssignableFrom(Double.class) || value.getClass().isAssignableFrom(Integer.class)) {
                        results.add(graph.vertices());
                    } else if (value.getClass().isAssignableFrom(String.class)) {
                        // Find index.
                        final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo =
                                graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", value);

                        // If we have index, query it, otherwise we need to scan (or error out).
                        final P<?> predicate = P.eq(value);
                        if (propertyIndexInfo.isPresent()) {
                            results.add(GraphQuery.create(graph).queryVertexSIndex(propertyIndexInfo.get(), predicate,
                                    graph::vertexFromRecord, evaluationTimeout));
                        } else {
                            LOG.debug("No index found for vertex label, running scan");
                            results.add(GraphQuery.create(graph).scanSet(null,
                                    graph.getBaseGraph().VERTEX_AERO_SET,graph.getBaseGraph().LABEL_BIN, predicate,
                                    graph::vertexFromRecord, evaluationTimeout));
                        }
                    } else {
                        results.add(graph.vertices());
                    }
                } else {
                    if (value.getClass().isAssignableFrom(Long.class) || value.getClass().isAssignableFrom(Double.class) ||
                            value.getClass().isAssignableFrom(Integer.class) || value.getClass().isAssignableFrom(String.class)) {
                        // Find index.
                        final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, key.toString(), value);

                        // If we have index, query it, otherwise we need to scan (or error out).
                        final Iterator<? extends Vertex> iterator;
                        if (propertyIndexInfo.isPresent()) {
                            iterator = GraphQuery.create(graph).queryVertexSIndex(propertyIndexInfo.get(), P.eq(value),
                                    graph::vertexFromRecord, evaluationTimeout);
                        } else {
                            LOG.debug("No index found for key {} and value {}, running scan", key.toString(), value);
                            iterator = GraphQuery.create(graph).scanSet(key.toString(), graph.getBaseGraph().VERTEX_AERO_SET,
                                    graph.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN, P.eq(value),
                                    graph::vertexFromRecord, evaluationTimeout);
                        }
                        results.add(iterator);
                    } else {
                        results.add(graph.vertices());
                    }
                }
            });
            // Use the index if possible otherwise just in memory filter
            stream = FireflyCloseableIteratorUtils.stream(FireflyCloseableIteratorUtils.concat(results.toArray(new Iterator[0])));
        }

        stream = stream.filter(v -> {
            // Try to match on all search criteria skipping T.id as it was handled above
            return search.entrySet().stream().filter(kv -> {
                final Object k = kv.getKey();
                boolean res = k != T.id;
                return res;
            }).allMatch(kv -> {
                if (kv.getKey() == T.label) {
                    return v.label().equals(kv.getValue());
                } else {
                    final VertexProperty<Object> vp = v.property(kv.getKey().toString());
                    return vp.isPresent() && kv.getValue().equals(vp.value());
                }
            });
        });
        return stream.distinct();
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<S> traverser) {
        final Map mergeMap = materializeMap(traverser, mergeTraversal);;
        validateMapInput(mergeMap, false);
        while (true) {
            try {
                Stream<Vertex> stream = createSearchStream(mergeMap);
                stream = stream.map(v -> {
                    // If no onMatch is defined then there is no update - return the vertex unchanged
                    if (null == onMatchTraversal) return v;

                    // If this was a start step the traverser is initialized with Boolean/false, so override that with
                    // the matched Vertex so that the option() traversal can operate on it properly
                    if (isStart) traverser.set((S) v);

                    // Assume good input from GraphTraversal - folks might drop in a T here even though it is immutable
                    final Map<String, Object> onMatchMap = materializeMap(traverser, onMatchTraversal);
                    validateMapInput(onMatchMap, true);

                    if (onMatchMap != null) {
                        onMatchMap.forEach((key, value) -> {
                            // Trigger callbacks for eventing - in this case, it's a VertexPropertyChangedEvent. if there's no
                            // registry/callbacks then just set the property
                            if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                                final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                                final Property<?> p = v.property(key);
                                final Property<Object> oldValue = p.isPresent() ? eventStrategy.detach(v.property(key)) : null;
                                final Event.VertexPropertyChangedEvent vpce = new Event.VertexPropertyChangedEvent(eventStrategy.detach(v), oldValue, value);
                                this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
                            }

                            // Try to detect proper cardinality for the key according to the graph
                            final Graph graph = this.getTraversal().getGraph().get();
                            VertexProperty.Cardinality effectiveCard;
                            if (FireflyCloseableIteratorUtils.count(v.properties(key)) <= 1)
                                effectiveCard = VertexProperty.Cardinality.single;
                            else effectiveCard = VertexProperty.Cardinality.list;
                            v.property(effectiveCard, key, value);
                        });
                    }

                    return v;
                });

                // If the stream has something then there is a match (possibly updated) and is returned, otherwise a new
                // vertex is created
                final Iterator<Vertex> vertices = stream.iterator();
                if (vertices.hasNext()) {
                    return vertices;
                } else {
                    final Vertex vertex;

                    final Map<?, ?> onCreateMap = onCreateMap(traverser, mergeMap);
                    if (onCreateMap.isEmpty()) {
                        return Collections.emptyIterator();
                    }
                    final List<Object> keyValues = new ArrayList<>();
                    for (Map.Entry<?, ?> entry : onCreateMap.entrySet()) {
                        keyValues.add(entry.getKey());
                        keyValues.add(entry.getValue());
                    }
                    vertex = this.getTraversal().getGraph().get().addVertex(keyValues.toArray(new Object[keyValues.size()]));

                    // Trigger callbacks for eventing - in this case, it's a VertexAddedEvent
                    if (this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()) {
                        final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                        final Event.VertexAddedEvent vae = new Event.VertexAddedEvent(eventStrategy.detach(vertex));
                        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
                    }

                    return FireflyCloseableIteratorUtils.of(vertex);
                }
            } catch (final IllegalArgumentException e) {
                if (!e.getMessage().contains("Vertex with id already exists:")) {
                    throw e;
                }
            }
        }
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public FireflyMergeVertexStep<S> clone() {
        final FireflyMergeVertexStep<S> clone = (FireflyMergeVertexStep<S>) super.clone();
        clone.mergeTraversal = mergeTraversal.clone();
        clone.onCreateTraversal = onCreateTraversal != null ? onCreateTraversal.clone() : null;
        clone.onMatchTraversal = onMatchTraversal != null ? onMatchTraversal.clone() : null;
        return clone;
    }
}
