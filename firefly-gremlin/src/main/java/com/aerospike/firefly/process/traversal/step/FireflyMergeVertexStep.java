package com.aerospike.firefly.process.traversal.step;

import com.aerospike.client.Key;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Based on TinkerMergeVertexStep
 *
 * @author Stephen Mallette
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyMergeVertexStep<S> extends MergeVertexStep<S> {
    public FireflyMergeVertexStep(final MergeVertexStep step) {
        super(step.getTraversal(), step.isStart(), step.getSearchCreateTraversal());
        if (step.getOnMatchTraversal() != null) this.addChildOption(Merge.onMatch, step.getOnMatchTraversal());
        if (step.getOnCreateTraversal() != null) this.addChildOption(Merge.onCreate, step.getOnCreateTraversal());
        if (step.getCallbackRegistry() != null) this.callbackRegistry = step.getCallbackRegistry();
    }

    @Override
    protected Stream<Vertex> createSearchStream(final Map<Object, Object> search) {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
        Optional<String> firstIndex = Optional.empty();

        Stream<Vertex> stream;
        // prioritize lookup by id but otherwise attempt an index lookup
        if (null == search) {
            return Stream.empty();
        } else if (search.containsKey(T.id)) {
            Object sid = search.get(T.id);
            long lid;
            if (Integer.class.isAssignableFrom(sid.getClass()))
                lid = Long.valueOf((Integer) sid);
            else if (Long.class.isAssignableFrom(sid.getClass()))
                lid = (Long) sid;
            else throw new RuntimeException("unsupported id type");
            if (graph.getBaseGraph().exists(
                    new Key(graph.getBaseGraph().getNamespace(),
                            graph.getBaseGraph().VERTEX_AERO_SET,
                            lid)))
                return IteratorUtils.stream(graph.vertices(search.get(T.id)));
            else
                stream = Stream.empty();
        } else {
            List<Iterator<? extends Vertex>> results = new ArrayList<>();
            search.forEach((key, value) -> {
                if (key == T.label) {
                    if (value.getClass().isAssignableFrom(Long.class) || value.getClass().isAssignableFrom(Double.class) || value.getClass().isAssignableFrom(Integer.class)) {
                        results.add(graph.vertices());
                    } else if (value.getClass().isAssignableFrom(String.class)) {
                        results.add(FireflyHelper.queryVertexByLabelStringIndex(graph, value));
                    } else {
                        results.add(graph.vertices());
                    }
                } else {
                    if (value.getClass().isAssignableFrom(Long.class) || value.getClass().isAssignableFrom(Double.class) || value.getClass().isAssignableFrom(Integer.class)) {
                        results.add(FireflyHelper.queryVertexByVertexPropertyNumericIndex(graph, key.toString(), P.eq(value)));
                    } else if (value.getClass().isAssignableFrom(String.class)) {
                        Iterator<? extends Vertex> test = FireflyHelper.queryVertexByVertexPropertyStringIndex(graph, key.toString(), value);
                        results.add(FireflyHelper.queryVertexByVertexPropertyStringIndex(graph, key.toString(), value));
                    } else {
                        results.add(graph.vertices());
                    }
                }
            });
            // use the index if possible otherwise just in memory filter
            stream = IteratorUtils.stream(IteratorUtils.concat(results.toArray(new Iterator[0])));
        }

        stream = stream.filter(v -> {
            // try to match on all search criteria skipping T.id as it was handled above
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
}
