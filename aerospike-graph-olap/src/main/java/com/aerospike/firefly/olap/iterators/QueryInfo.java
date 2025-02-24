package com.aerospike.firefly.olap.iterators;

import com.aerospike.client.exp.Expression;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryInfo implements Serializable {
    public enum QueryType implements Serializable {
        INDEX,
        SCAN,
        PI
    }

    public final QueryInfo.QueryType queryType;
    public final List<HasContainer> fireflyHasContainers;
    public final FireflyIndexMetadata.IndexInfo indexInfo; // Only valid for index queries.
    public final HasContainer indexTopHasContainer; // Only valid for index queries.
    public final List<Object> ids; // Only valid for PI queries.
    public final Expression expression; // Only valid for scan queries.

    private QueryInfo(final FireflyIndexMetadata.IndexInfo indexInfo,
                      final HasContainer hasContainer,
                      final List<HasContainer> initialHasContainers) {
        this.queryType = QueryInfo.QueryType.INDEX;
        this.indexInfo = indexInfo;
        this.indexTopHasContainer = hasContainer;
        this.ids = null;
        this.expression = null;
        this.fireflyHasContainers = initialHasContainers;
    }

    private QueryInfo(final List<Object> ids,
                      final List<HasContainer> initialHasContainers) {
        this.queryType = QueryInfo.QueryType.PI;
        this.indexInfo = null;
        this.indexTopHasContainer = null;
        this.ids = ids;
        this.expression = null;
        this.fireflyHasContainers = initialHasContainers;
    }

    private QueryInfo(final Expression expression,
                      final List<HasContainer> initialHasContainers) {
        this.queryType = QueryInfo.QueryType.SCAN;
        this.indexInfo = null;
        this.indexTopHasContainer = null;
        this.ids = null;
        this.expression = expression;
        this.fireflyHasContainers = initialHasContainers;
    }

    private QueryInfo(final List<HasContainer> initialHasContainers) {
        this.queryType = QueryInfo.QueryType.SCAN;
        this.indexInfo = null;
        this.indexTopHasContainer = null;
        this.ids = null;
        this.expression = null; // Scan all.
        this.fireflyHasContainers = initialHasContainers;
    }

    public static QueryInfo getQueryInfo(final FireflyGraph graph,
                                         final GraphStep step,
                                         final List<HasContainer> initialHasContainers,
                                         final Object[] idsInput) {
        final List<HasContainer> positiveFilters = initialHasContainers.stream().filter(it -> !it.getBiPredicate().equals(Contains.without)).collect(Collectors.toList());

        // Special case.
        if (idsInput == null) {
            return null;
        }
        if (idsInput != null && idsInput.length > 0) {
            final List<Object> ids = Stream.of(idsInput).collect(Collectors.toList());
            return new QueryInfo(ids, initialHasContainers);
        }

        // Check for PI query based on hasContainers.
        if (!positiveFilters.isEmpty()) {
            final List<Object> ids = positiveFilters
                    .stream()
                    .filter(it -> "~id".equals(it.getKey()))
                    .map(HasContainer::getValue)
                    .flatMap(it -> it instanceof List ? ((List<?>) it).stream() : Stream.of(it))
                    .collect(Collectors.toList());
            final List<HasContainer> nonIdContainers = initialHasContainers.stream().filter(it -> !"~id".equals(it.getKey())).collect(Collectors.toList());

            // If there are id has containers, we can do a batch read.
            if (!ids.isEmpty()) {
                return new QueryInfo(ids, nonIdContainers);
            }
        }

        if (step.returnsVertex()) {
            final List<FireflyGraphStep.HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, FireflyVertex.class, initialHasContainers);
            final List<HasContainer> aerospikeSideHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(sortedHasContainers);
            final List<HasContainer> fireflySideHasContainers = FireflyBatchReadHelper.getFireflyHasContainers(sortedHasContainers);
            final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.get(0);
            final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo = topContainer != null ?
                    graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, topContainer.getKey(), topContainer.getValue()) :
                    Optional.empty();
            if (propertyIndexInfo.isPresent()) {
                // Index.
                // Remove top container since it is captured inside property index info.
                aerospikeSideHasContainers.remove(0);
                return new QueryInfo(propertyIndexInfo.get(), topContainer, initialHasContainers);
            } else {
                // Scan.
                final Expression expression = GraphQueryHelper.hasContainerListToExpression(graph.getBaseGraph(), aerospikeSideHasContainers, FireflyVertex.class);
                return new QueryInfo(expression, initialHasContainers);
            }
        } else {
            return new QueryInfo(initialHasContainers);
        }
    }
}
