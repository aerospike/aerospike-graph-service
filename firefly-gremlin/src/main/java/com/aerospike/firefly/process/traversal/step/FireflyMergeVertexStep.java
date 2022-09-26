package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyGraph;
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
            stream = IteratorUtils.stream(graph.vertices(search.get(T.id)));
        } else {
            List<Iterator<? extends Vertex>> results = new ArrayList<>();
            AtomicBoolean indexSupported = new AtomicBoolean(false);
            search.forEach((key, value) -> {
                if (!indexSupported.get()) {
                    if (value.getClass().isAssignableFrom(Number.class)) {
                        results.add(FireflyHelper.queryVertexByVertexPropertyNumericIndex(graph, key.toString(), P.eq(value)));
                        indexSupported.set(true);
                    } else if (value.getClass().isAssignableFrom(String.class)) {
                        results.add(FireflyHelper.queryVertexByVertexPropertyStringIndex(graph, key.toString(), value.toString()));
                        indexSupported.set(true);
                    }
                }
            });
            if (!indexSupported.get())
                results.add(graph.vertices());

            // use the index if possible otherwise just in memory filter
            stream = IteratorUtils.stream(IteratorUtils.concat(results.toArray(new Iterator[0])));
        }

        final Optional<String> indexUsed = firstIndex;
        stream = stream.filter(v -> {
            // try to match on all search criteria skipping T.id as it was handled above
            return search.entrySet().stream().filter(kv -> {
                final Object k = kv.getKey();
                return k != T.id && !(indexUsed.isPresent() && indexUsed.get().equals(k));
            }).allMatch(kv -> {
                if (kv.getKey() == T.label) {
                    return v.label().equals(kv.getValue());
                } else {
                    final VertexProperty<Object> vp = v.property(kv.getKey().toString());
                    return vp.isPresent() && kv.getValue().equals(vp.value());
                }
            });
        });

        return stream;
    }
}
