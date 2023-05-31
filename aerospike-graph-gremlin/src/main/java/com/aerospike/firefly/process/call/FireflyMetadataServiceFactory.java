package com.aerospike.firefly.process.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.util.FireflyGraphSummaryUpdater;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;

public class FireflyMetadataServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private final FireflyGraph graph;
    public static final String PRETTY_PRINT_FORMAT_LOG = "Total vertex count: {}.\n" +
            "Vertex count by label: {}.\n" +
            "Vertex properties by label: {}.\n" +
            "Total edge count: {}.\n" +
            "Edge count by label: {}.\n" +
            "Edge properties by label: {}.";
    public static final String PRETTY_PRINT_FORMAT_SYSTEM = "Total vertex count: %d.\n" +
            "Vertex count by label: %s.\n" +
            "Vertex properties by label: %s.\n" +
            "Total edge count: %d.\n" +
            "Edge count by label: %s.\n" +
            "Edge properties by label: %s.";

    public FireflyMetadataServiceFactory(final FireflyGraph graph) {
        this.graph = graph;
    }

    @Override
    public String getName() {
        return "summary";
    }

    @Override
    public Set<Service.Type> getSupportedTypes() {
        return null;
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public Type getType() {
        return null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        final FireflyGraphSummaryUpdater.FireflyElementMetadata fireflyElementMetadata =
                graph.fireflySummaryUpdater.getFireflyStatistics();
        if (params.containsKey("pretty") && params.get("pretty").equals(true)) {
            return (CloseableIterator<R>) FireflyCloseableIteratorUtils.of(
                    String.format(PRETTY_PRINT_FORMAT_SYSTEM,
                            fireflyElementMetadata.totalVertexCount(),
                            fireflyElementMetadata.vertexCountByLabel().toString(),
                            fireflyElementMetadata.vertexPropertiesByLabel().toString(),
                            fireflyElementMetadata.totalEdgeCount(),
                            fireflyElementMetadata.edgeCountByLabel().toString(),
                            fireflyElementMetadata.edgePropertiesByLabel().toString()));
        }
        return (CloseableIterator<R>) FireflyCloseableIteratorUtils.of(Map.of(
                "Total vertex count", fireflyElementMetadata.totalVertexCount(),
                "Vertex count by label", fireflyElementMetadata.vertexCountByLabel(),
                "Vertex properties by label", fireflyElementMetadata.vertexPropertiesByLabel(),
                "Total edge count", fireflyElementMetadata.totalEdgeCount(),
                "Edge count by label", fireflyElementMetadata.edgeCountByLabel(),
                "Edge properties by label", fireflyElementMetadata.edgePropertiesByLabel()));
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }
}
