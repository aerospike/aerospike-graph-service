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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalGraphComputerView {

    private final FireflyGraph graph;
    protected final Map<String, VertexComputeKey> computeKeys;
    private final Map<Element, Map<String, List<VertexProperty<?>>>> computeProperties;
    private final GraphFilter graphFilter;
    //private final Set<String> retainVertexProperties;

    public LocalGraphComputerView(final FireflyGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        this.graph = graph;
        this.computeKeys = new HashMap<>();
        computeKeys.forEach(key -> this.computeKeys.put(key.getKey(), key));
        this.computeProperties = new ConcurrentHashMap<>();
        this.graphFilter = graphFilter;
        /*if (this.graphFilter.hasVertexPropertyFilter()) {
            retainVertexProperties = new HashSet<>(Arrays.asList(((PropertiesStep) graphFilter.getVertexPropertyFilter().getStartStep()).getPropertyKeys()));
        } else {
            retainVertexProperties = null;
        }*/
    }

    public <V> Property<V> addProperty(final FireflyVertex vertex, final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        System.out.println("addProperty " + key);
        if (!getProperty(vertex, key).isEmpty()) {
            return new DetachedVertexProperty<>(99999999, key, value, Map.of(), vertex);
        }
        if (isComputeKey(key)) {
            final DetachedVertexProperty<V> property = new DetachedVertexProperty<>(99999999, key, value, Map.of(), vertex) {
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

    public List<VertexProperty<?>> getProperty(final FireflyVertex vertex, final String key) {
        // if the vertex property is already on the vertex, use that.
        final List<VertexProperty<?>> vertexProperty = this.getValue(vertex, key);
        return vertexProperty.isEmpty() ? (List) getPropertiesMap(vertex).getOrDefault(key, Collections.emptyList()) : vertexProperty;
    }

    public <V> List<VertexProperty<V>> getComputeProperties(final FireflyVertex vertex, final String... computeKeys) {
        final List<VertexProperty<V>> list = new ArrayList<>();
        for (final List<VertexProperty<?>> properties : this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).values()) {
            properties.stream().filter(p -> ElementHelper.keyExists(p.key(), computeKeys)).forEach(p -> list.add((VertexProperty<V>) p));
        }
        return list;
    }

    public <V> List<VertexProperty<V>> getLocalVertices(final String computeKey) {
        return computeProperties.values().stream().flatMap(m -> m.values().stream()).flatMap(List::stream).map(p -> (VertexProperty<V>) p).collect(Collectors.toList());
    }

    public List<Element> getLocalVertices2(final String computeKey) {
        return computeProperties.keySet().stream().collect(Collectors.toList());
    }


    /*public List<Property<?>> getProperties(final FireflyVertex vertex) {
        final List<Property<?>> list = new ArrayList<>();
        for (final List<VertexProperty<?>> properties : getPropertiesMap(vertex).values()) {
            list.addAll(properties);
        }
        for (final List<VertexProperty<?>> properties : this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).values()) {
            list.addAll(properties);
        }
        return list;
    }*/

    private Map<String, List<VertexProperty<?>>> getPropertiesMap(final FireflyVertex vertex) {
        Map<String, List<VertexProperty<?>>> propertiesMap = new HashMap<>();
        vertex.properties().forEachRemaining(prop -> {
            propertiesMap.put(prop.key(), List.of(prop));
        });
        //if (retainVertexProperties != null) {
        //    propertiesMap.keySet().retainAll(retainVertexProperties);
        // }
        return propertiesMap;
    }

    public void removeProperty(final DetachedVertex vertex, final String key, final VertexProperty<?> property) {
        if (isComputeKey(key)) {
            this.removeValue(vertex, key, property);
        } else {
            throw GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey(key);
        }
    }

    public boolean legalVertex(final Vertex vertex) {
        return  !this.graphFilter.hasVertexFilter() || TraversalUtil.test(vertex, this.graphFilter.getVertexFilter().clone());
        //return this.graphFilter.legalVertex(vertex);
    }


    public boolean legalEdge(final Vertex vertex, final Edge edge) {
        return this.legalVertex(vertex) && (this.graphFilter.checkEdgeLegality(Direction.OUT, edge.label()).positive() ||
                this.graphFilter.checkEdgeLegality(Direction.IN, edge.label()).positive() ||
                this.graphFilter.checkEdgeLegality(Direction.BOTH, edge.label()).positive());
        //return !this.graphFilter.hasEdgeFilter() || this.legalEdges.get(vertex.id()).contains(edge.id());
    }

    protected void complete() {
        // remove all transient properties from the vertices
        for (final VertexComputeKey computeKey : this.computeKeys.values()) {
            if (computeKey.isTransient()) {
                for (final Map<String, List<VertexProperty<?>>> properties : this.computeProperties.values()) {
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
                /*final FireflyGraph newGraph = FireflyGraph.open(this.graph.configuration());
                this.graph.vertices().forEachRemaining(vertex -> {
                    final Vertex newVertex = newGraph.addVertex(T.id, vertex.id(), T.label, vertex.label());
                    vertex.properties().forEachRemaining(vertexProperty -> {
                        final VertexProperty<?> newVertexProperty = newVertex.property(VertexProperty.Cardinality.list, vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());
                        vertexProperty.properties().forEachRemaining(property -> {
                            newVertexProperty.property(property.key(), property.value());
                        });
                    });
                });
                return newGraph;*/
            }
        } else {  // Persist.EDGES
            if (GraphComputer.ResultGraph.ORIGINAL == resultGraph) {
                this.addPropertiesToOriginalGraph();
                return this.graph;
            } else {
                throw new UnsupportedOperationException("Persisting edges to new graph currently not supported");
                /*final FireflyGraph newGraph = FireflyGraph.open(this.graph.configuration());
                this.graph.vertices().forEachRemaining(vertex -> {
                    final Vertex newVertex = newGraph.addVertex(T.id, vertex.id(), T.label, vertex.label());
                    vertex.properties().forEachRemaining(vertexProperty -> {
                        final VertexProperty<?> newVertexProperty = newVertex.property(VertexProperty.Cardinality.list, vertexProperty.key(), vertexProperty.value(), T.id, vertexProperty.id());
                        vertexProperty.properties().forEachRemaining(property -> {
                            newVertexProperty.property(property.key(), property.value());
                        });
                    });
                });
                this.graph.edges().forEachRemaining(edge -> {
                    final Vertex outVertex = newGraph.vertices(edge.outVertex().id()).next();
                    final Vertex inVertex = newGraph.vertices(edge.inVertex().id()).next();
                    final Edge newEdge = outVertex.addEdge(edge.label(), inVertex, T.id, edge.id());
                    edge.properties().forEachRemaining(property -> newEdge.property(property.key(), property.value()));
                });
                return newGraph;*/
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
        final Map<String, List<VertexProperty<?>>> elementProperties = this.computeProperties.computeIfAbsent(vertex, k -> new HashMap<>());
        elementProperties.compute(key, (k, v) -> {
            if (null == v) v = new ArrayList<>();
            v.add(property);
            return v;
        });
    }

    private void removeValue(final Vertex vertex, final String key, final VertexProperty<?> property) {
        this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).get(key).remove(property);
    }

    private List<VertexProperty<?>> getValue(final Vertex vertex, final String key) {
        return this.computeProperties.getOrDefault(vertex, Collections.emptyMap()).getOrDefault(key, Collections.emptyList());
    }
}
