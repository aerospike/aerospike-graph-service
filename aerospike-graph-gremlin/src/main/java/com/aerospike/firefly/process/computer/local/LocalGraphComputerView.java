package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalGraphComputerView {

    private final FireflyGraph graph;
    protected final Map<String, VertexComputeKey> computeKeys;
    private final Map<Element, Map<String, Queue<VertexProperty<?>>>> computeProperties;
    private final GraphFilter graphFilter;

    public LocalGraphComputerView(final FireflyGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        this.graph = graph;
        this.computeKeys = new ConcurrentHashMap<>();
        computeKeys.forEach(key -> this.computeKeys.put(key.getKey(), key));
        this.computeProperties = new ConcurrentHashMap<>();
        this.graphFilter = graphFilter;
    }

    public <V> Property<V> addProperty(final FireflyVertex vertex, final String key, final V value) {
        synchronized (vertex) {
            ElementHelper.validateProperty(key, value);
            if (!getProperty(vertex, key).isEmpty()) {
                return new DetachedVertexProperty<>(1, key, value, Map.of(), vertex);
            }
            if (isComputeKey(key)) {
                final DetachedVertexProperty<V> property = new DetachedVertexProperty<>(1, key, value, Map.of(), vertex) {
                    @Override
                    public void remove() {
                        removeProperty(vertex, key, this);
                    }
                };
                this.addValue(vertex, key, property);
                return property;
            } else {
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            }
        }
    }

    public List<VertexProperty<?>> getProperty(final FireflyVertex vertex, final String key) {
        synchronized (vertex) {
            final List<VertexProperty<?>> vertexProperty = this.getValue(vertex, key);
            final List<VertexProperty<?>> vps;
            if (vertexProperty.isEmpty()) {
                vps = getPropertiesMap(vertex).getOrDefault(key, Collections.emptyList());
            } else {
                vps = vertexProperty;
            }
            final List<VertexProperty<?>> list = new ArrayList<>(vps);
            return list;
        }
    }

    public <V> List<VertexProperty<V>> getComputeProperties(final FireflyVertex vertex, final String... computeKeys) {
        synchronized (vertex) {
            final List<VertexProperty<V>> list = new ArrayList<>();
            for (final Queue<VertexProperty<?>> properties : this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).values()) {
                for (final VertexProperty<?> property : properties) {
                    if (ElementHelper.keyExists(property.key(), computeKeys)) {
                        list.add((VertexProperty<V>) property);
                    }
                }
            }
            return list;
        }
    }

    private Map<String, List<VertexProperty<?>>> getPropertiesMap(final FireflyVertex vertex) {
        final Map<String, List<VertexProperty<?>>> propertiesMap = new ConcurrentHashMap<>();
        vertex.properties().forEachRemaining(prop -> {
            propertiesMap.put(prop.key(), List.of(prop));
        });
        return propertiesMap;
    }

    public void removeProperty(final DetachedVertex vertex, final String key, final VertexProperty<?> property) {
        synchronized (vertex) {
            if (isComputeKey(key)) {
                this.removeValue(vertex, key, property);
            } else {
                throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
            }
        }
    }

    public boolean legalVertex(final Vertex vertex) {
        return !this.graphFilter.hasVertexFilter() || TraversalUtil.test(vertex, this.graphFilter.getVertexFilter().clone());
    }


    public boolean legalEdge(final Vertex vertex, final Edge edge) {
        return this.legalVertex(vertex) && (this.graphFilter.checkEdgeLegality(Direction.OUT, edge.label()).positive() ||
                this.graphFilter.checkEdgeLegality(Direction.IN, edge.label()).positive() ||
                this.graphFilter.checkEdgeLegality(Direction.BOTH, edge.label()).positive());
    }

    protected void complete() {
        // remove all transient properties from the vertices
        for (final VertexComputeKey computeKey : this.computeKeys.values()) {
            if (computeKey.isTransient()) {
                for (final Map<String, Queue<VertexProperty<?>>> properties : this.computeProperties.values()) {
                    properties.remove(computeKey.getKey());
                }
            }
        }
    }

    public Graph processResultGraphPersist(final GraphComputer.ResultGraph resultGraph,
                                           final GraphComputer.Persist persist) {
        if (GraphComputer.Persist.NOTHING == persist) {
            if (GraphComputer.ResultGraph.ORIGINAL == resultGraph)
                return this.graph;
            else
                return EmptyGraph.instance();
        } else if (GraphComputer.Persist.VERTEX_PROPERTIES == persist) {
            if (GraphComputer.ResultGraph.ORIGINAL == resultGraph) {
                this.addPropertiesToOriginalGraph();
                return this.graph;
            } else {
                throw new UnsupportedOperationException("Persisting properties to new graph currently not supported");
            }
        } else {  // Persist.EDGES
            if (GraphComputer.ResultGraph.ORIGINAL == resultGraph) {
                this.addPropertiesToOriginalGraph();
                return this.graph;
            } else {
                throw new UnsupportedOperationException("Persisting edges to new graph currently not supported");
            }
        }
    }

    private void addPropertiesToOriginalGraph() {
        FireflyHelper.dropGraphComputerView(this.graph);
        this.computeProperties.forEach((element, properties) -> {
            properties.forEach((key, vertexProperties) -> {
                vertexProperties.forEach(vertexProperty -> {
                    final VertexProperty<?> newVertexProperty = ((Vertex) element).property(VertexProperty.Cardinality.list, vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());
                    vertexProperty.properties().forEachRemaining(property -> {
                        newVertexProperty.property(property.key(), property.value());
                    });
                });
            });
        });
        this.computeProperties.clear();
    }

    private boolean isComputeKey(final String key) {
        return this.computeKeys.containsKey(key);
    }

    private void addValue(final Vertex vertex, final String key, final VertexProperty<?> property) {
        final Map<String, Queue<VertexProperty<?>>> elementProperties = this.computeProperties.computeIfAbsent(vertex, k -> new ConcurrentHashMap<>());
        elementProperties.compute(key, (k, v) -> {
            if (null == v) {
                v = new ConcurrentLinkedQueue<>();
            } else {
                v.clear();
            }
            v.add(property);
            return v;
        });
    }

    private void removeValue(final Vertex vertex, final String key, final VertexProperty<?> property) {
        final Queue<VertexProperty<?>> lvp = this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).get(key);
        if (lvp != null) {
            lvp.remove(property);
        }
    }

    private List<VertexProperty<?>> getValue(final Vertex vertex, final String key) {
        if (this.computeProperties.containsKey(vertex)) {
            final Queue<VertexProperty<?>> result = this.computeProperties.get(vertex).getOrDefault(key, new ConcurrentLinkedQueue<>());
            return new ArrayList<>(result);
        } else {
            return Collections.emptyList();
        }
    }
}
