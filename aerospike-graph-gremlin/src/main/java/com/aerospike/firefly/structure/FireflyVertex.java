package com.aerospike.firefly.structure;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.structure.id.LazyIdTransform;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.structure.iterator.FireflyFilteredBatchEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromIndexedVertex;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.aerospike.firefly.util.exceptions.TtlArgumentException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getTypeHintOf;
import static com.aerospike.firefly.process.traversal.step.util.TraversalUtil.fireflyTestAll;
import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyVertex extends FireflyElement implements Vertex {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertex.class);
    protected final Map<String, List<LazyIdTransform>> inEdgeIds;
    protected final Map<String, List<LazyIdTransform>> outEdgeIds;
    protected final AerospikeConnection db;
    protected FireflyGraph graph;
    public static final String SUPERNODE_PROPERTY_KEY = "~supernode";
    protected Map<Long, HashMap<Object, List<Long>>> vertexProperties;
    protected Map<Long, Map<Long, Object>> vpTypeHints;
    protected Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties;
    protected boolean isEdgeCacheOverflowed;

    public FireflyVertex(final FireflyId fid,
                         final String label,
                         final FireflyGraph graph,
                         final Map<String, List<LazyIdTransform>> inEdgeIds,
                         final Map<String, List<LazyIdTransform>> outEdgeIds,
                         final Map<Long, HashMap<Object, List<Long>>> vertexProperties,
                         final Map<Long, Map<Long, Object>> vpTypeHints,
                         final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties,
                         final boolean isEdgeCacheOverflowed) {
        super(fid, label);
        this.graph = graph;
        this.inEdgeIds = inEdgeIds == null ? new TreeMap<>() : inEdgeIds;
        this.outEdgeIds = outEdgeIds == null ? new TreeMap<>() : outEdgeIds;
        this.vertexProperties = vertexProperties == null ? new TreeMap<>() : vertexProperties;
        this.vpTypeHints = vpTypeHints == null ? new HashMap<>() : vpTypeHints;
        this.vpProperties = vpProperties == null ? new HashMap<>() : vpProperties;
        this.isEdgeCacheOverflowed = isEdgeCacheOverflowed;
        this.db = graph.getBaseGraph();
    }

    /**
     * Get the vertex property by vertex property label for the vertex.
     *
     * @param key vertex property label.
     * @return Iterator of FireflyVertexProperty for provided vertex property label.
     */
    protected <V> Iterator<VertexProperty<V>> readVertexProperty(final String key) {
        LOG.debug("Reading vertex property {}", key);

        if (SUPERNODE_PROPERTY_KEY.equals(key)) {
            if (this.isEdgeCacheOverflowed) {
                return FireflyCloseableIteratorUtils.of(new FireflyVirtualSupernodeVertexProperty<>(this));
            } else {
                return Collections.emptyIterator();
            }
        }

        final Long schemaPropertyKey = this.db.schemaManager.getVertexPropertyRead(key);
        if (!this.vertexProperties.containsKey(schemaPropertyKey)) {
            return Collections.emptyIterator();
        }

        final List<VertexProperty<V>> vertexProperties = new ArrayList<>();
        for (final Map.Entry<Object, List<Long>> valueToIdList : this.vertexProperties.get(schemaPropertyKey).entrySet()) {
            for (final Long vpId : valueToIdList.getValue()) {
                final FireflyId fireflyVpId = this.db.getIdFactory().createVertexPropertyId(vpId);
                final Map<Long, List<Object>> vpPropertyMap = this.vpProperties.get(schemaPropertyKey).get(vpId);
                final Object typeHint = this.vpTypeHints.get(schemaPropertyKey).get(vpId);
                final V convertedValue = (V) this.db.convertValueToTypeUsingHint(valueToIdList.getKey(), typeHint);
                final VertexProperty<V> vertexProperty = new FireflyVertexProperty<>(graph, fireflyVpId, this, key, convertedValue, vpPropertyMap);
                vertexProperties.add(vertexProperty);
            }
        }

        return vertexProperties.iterator();
    }

    public void updateVertexPropertyJVMCache(final Map<Long, HashMap<Object, List<Long>>> vertexProperties,
                                             final Map<Long, Map<Long, Object>> vpTypeHints,
                                             final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties) {
        this.vertexProperties = vertexProperties == null ? new TreeMap<>() : vertexProperties;
        this.vpTypeHints = vpTypeHints == null ? new HashMap<>() : vpTypeHints;
        this.vpProperties = vpProperties == null ? new HashMap<>() : vpProperties;
    }

    /**
     * Remove edge from vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     */
    public FireflyId removeEdge(final Direction direction, final FireflyPhatEdgeId edgeId, final String edgeLabel) {

        // Update the JVM cache of this.
        final Map<String, List<LazyIdTransform>> edgeCache = direction == Direction.IN ? this.inEdgeIds : this.outEdgeIds;

        // If the edge is not in the JVM cache it also means it wasn't read from DB, so no need to operate on DB to
        // remove what isn't there.
        if (!edgeCache.containsKey(edgeLabel)) {
            if (!this.isEdgeCacheOverflowed) {
                LOG.error("Could not find edge label {} in vertex {}. Vertex edge cache did not contain edge id {}.",
                        edgeLabel, this.id, edgeId);
            }
            return null;
        }
        final List<FireflyId> edgeIdsOfLabel = edgeCache.get(edgeLabel).stream().map(LazyIdTransform::transform).collect(Collectors.toList());
        final int indexOfEdgeToRemove = edgeIdsOfLabel.indexOf(edgeId);
        if (indexOfEdgeToRemove == -1) {
            if (!this.isEdgeCacheOverflowed) {
                LOG.error("Could not find edge id {} in vertex {}. Vertex edge cache under label {} did not contain edge id {}.",
                        edgeId, this.id, edgeLabel, edgeId);
            }
            return null;
        }

        // Remove item from vertex property Map.
        final FireflyId compositeIdToRemove = edgeIdsOfLabel.remove(indexOfEdgeToRemove);

        // Remove key if IDs are empty.
        if (edgeIdsOfLabel.isEmpty()) {
            edgeCache.remove(edgeLabel);
        }

        return compositeIdToRemove;
    }

    /**
     * Write edge to vertex.
     *
     * @param direction Direction of edge.
     * @param edgeId    Id of edge.
     * @param edgeLabel Label of edge.
     * @return was the edge written to this vertex's edge cache.
     */
    public boolean writeEdge(final Direction direction, final FireflyId edgeId, final String edgeLabel) {
        // Edge cache is overflowed for this vertex - do nothing since writing to overflow bin is on the edge record.
        if (this.isEdgeCacheOverflowed) {
            return false;
        }

        // Update this object's cache in JVM.
        if (direction == Direction.IN) {
            if (!this.inEdgeIds.containsKey(edgeLabel)) {
                this.inEdgeIds.put(edgeLabel, new ArrayList<>());
            }
            this.inEdgeIds.get(edgeLabel).add(new LazyIdTransform(edgeId));
        } else {
            if (!this.outEdgeIds.containsKey(edgeLabel)) {
                this.outEdgeIds.put(edgeLabel, new ArrayList<>());
            }
            this.outEdgeIds.get(edgeLabel).add(new LazyIdTransform(edgeId));
        }
        return true;
    }

    /**
     * Remove vertex. Any edges attached to adjacent vertices must be removed
     * from the adjacent vertices when the edge is removed.
     */
    @Override
    public void remove() {
        graph.aerospikeOperations.removeVertex(this);

        // Set flags to indicate vertex has been removed.
        this.removed = true;
    }

    public Iterator<FireflyId> getEdgeIdsFromVertex(final Direction direction, final Set<String> labels,
                                                    final List<HasContainer> hasContainers) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> cachedIds = getCachedEdgeIds(direction, labels);

        if (isEdgeCacheOverflowed) {
            return FireflyCloseableIteratorUtils.concat(cachedIds.iterator(), getSupernodeEdgeIds(direction, labels, hasContainers));
        } else {
            return cachedIds.iterator();
        }
    }

    public Iterator<FireflyId> getEdgeIdsFromVertex(final Direction direction, final Set<String> labels,
                                                    final List<HasContainer> supernodeContainers, final List<HasContainer> adjustedIdContainers) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> cachedIds = getCachedEdgeIds(direction, labels, adjustedIdContainers);

        if (isEdgeCacheOverflowed) {
            return FireflyCloseableIteratorUtils.concat(cachedIds.iterator(), getSupernodeEdgeIds(direction, labels, supernodeContainers, adjustedIdContainers));
        } else {
            return cachedIds.iterator();
        }
    }

    /**
     * Get all edge ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public List<FireflyId> getBatchedEdgeIdsFromVertex(final Direction direction, final Set<String> labels, final List<FireflyId> edgeIdContainer,
                                                       final List<HasContainer> supernodeHasContainers, final List<HasContainer> adjustedIdContainers) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> edgeIds;
        if (edgeIdContainer == null) {
            edgeIds = new ArrayList<>();
        } else {
            edgeIds = edgeIdContainer;
        }

        edgeIds.addAll(getCachedEdgeIds(direction, labels, adjustedIdContainers));

        if (isEdgeCacheOverflowed) {
            final Iterator<FireflyId> superNodeEdgeIds = getSupernodeEdgeIds(direction, labels, supernodeHasContainers, adjustedIdContainers);
            superNodeEdgeIds.forEachRemaining(edgeIds::add);
        }

        return edgeIds;
    }

    /**
     * Get all edge ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public List<FireflyId> getBatchedEdgeIdsFromVertex(final Direction direction, final Set<String> labels, final List<FireflyId> edgeIdContainer,
                                                       final List<HasContainer> supernodeContainers, final Map<FireflyId, FireflyEdge> edgeCache) {
        LOG.trace("Getting edge ids from vertex {}.", id);
        final List<FireflyId> edgeIds;
        if (edgeIdContainer == null) {
            edgeIds = new ArrayList<>();
        } else {
            edgeIds = edgeIdContainer;
        }

        edgeIds.addAll(getCachedEdgeIds(direction, labels));
        if (edgeCache != null) {
            final List<FireflyEdge> edges = graph.readEdges(Collections.emptyList(), edgeIds, null);
            for (final FireflyEdge edge : edges) {
                if (fireflyTestAll(edge, supernodeContainers)) {
                    edgeCache.put(edge.id, edge);
                }
            }
        }
        if (isEdgeCacheOverflowed) {
            final Iterator<FireflyId> superNodeEdgeIds = getSupernodeEdgeIds(direction, labels, supernodeContainers, edgeCache);
            superNodeEdgeIds.forEachRemaining(edgeIds::add);
        }

        return edgeIds;
    }

    /**
     * Used by FireflyMergeEdgeStep. Leverage adjacency sindex filtering to find any edges between this and a given
     * vertex id.
     *
     * @param direction         Direction to get edges for.
     * @param adjacent          Id of adjacent vertex to find edges between.
     * @param label             Label that edges must match.
     * @param propertyEqFilters Property key-value pairs that edges must match.
     * @return Iterator of edges between this vertex and adjacent that match all filters.
     */
    public CloseableIterator<Edge> getEdgesAdjacentToVertex(final Direction direction, final FireflyId adjacent,
                                                            final String label,
                                                            final Map<String, Object> propertyEqFilters) {
        LOG.debug("Getting Edges from Vertex {} with direction {} and adjacent Vertex {}", this.id, direction, adjacent);
        final Set<String> labels = label == null ? Collections.emptySet() : Collections.singleton(label);
        final List<FireflyId> edgeIds = new ArrayList<>();
        final List<FireflyId> cachedIds = getCachedIds(direction, labels);
        for (final FireflyId id : cachedIds) {
            final FireflyIdComposite composite = (FireflyIdComposite) id;
            if (adjacent.equals(composite.getAdjacentId())) {
                edgeIds.add(composite.getEdgeId());
            }
        }

        final List<HasContainer> fireflyHasContainers = new ArrayList<>();
        final List<HasContainer> aerospikeHasContainers = new ArrayList<>();
        for (final Map.Entry<String, Object> property : propertyEqFilters.entrySet()) {
            final Class valueClass = property.getValue().getClass();
            final HasContainer hasContainer;
            if (P.class.isAssignableFrom(valueClass)) {
                // TODO: This is dumb but ensures that we replicate default Tinkerpop behavior. MergeEdge is functional
                //       when a Predicate is passed in and a match is found, but breaks when a match isn't found because
                //       it tries to write a Predicate as a value on creation of the new Edge. Fix this when, if ever,
                //       Tinkerpop fixes it so that our behavior matches.
                hasContainer = new HasContainer(property.getKey(), (P<?>) property.getValue());
            } else {
                hasContainer = new HasContainer(property.getKey(), P.eq(property.getValue()));
            }
            fireflyHasContainers.add(hasContainer);
            if (Long.class.isAssignableFrom(valueClass) || Integer.class.isAssignableFrom(valueClass) ||
                    String.class.isAssignableFrom(valueClass)) {
                aerospikeHasContainers.add(hasContainer);
            }
        }
        final Iterator<FireflyId> adjacentEdgeIds;
        if (isEdgeCacheOverflowed) {
            final Iterator<FireflyId> sindexEdgeIds = new FireflyPhatEdgeIdIteratorFromIndexedVertex(
                    graph.aerospikeOperations.getEdgeKeyRecordsByIndex(this.id, direction, labels,
                            FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, aerospikeHasContainers, adjacent),
                    this.graph, direction, this.id, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, adjacent, null);
            adjacentEdgeIds = FireflyCloseableIteratorUtils.concat(edgeIds.iterator(), sindexEdgeIds);
        } else {
            adjacentEdgeIds = edgeIds.iterator();
        }
        return new FireflyFilteredBatchEdgeIterator<>(graph, adjacentEdgeIds, fireflyHasContainers);
    }

    /**
     * Get all adjacent vertex ids from vertex for given Direction. This considers supernode ids and cached ids,
     * handling index/scanning under the hood.
     *
     * @param direction Direction to get edge ids for.
     * @return Iterator of all edge ids.
     */
    public Iterator<FireflyId> getVertexIdsFromVertex(final Direction direction, final Set<String> labels) {
        LOG.trace("Getting vertex ids from vertex {}.", id);
        final List<FireflyId> cachedIds = getCachedVertexIds(direction, labels);

        if (isEdgeCacheOverflowed) {
            return FireflyCloseableIteratorUtils.concat(cachedIds.iterator(), getSupernodeVertexIds(direction, labels));
        } else {
            return cachedIds.iterator();
        }
    }

    public Iterator<FireflyId> getSupernodeEdgeIds(final Direction direction, final Set<String> labels,
                                                   final List<HasContainer> hasContainers) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, hasContainers, (List<HasContainer>) null);
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedEdgeIds.
     */
    public Iterator<FireflyId> getSupernodeEdgeIds(final Direction direction, final Set<String> labels,
                                                   final List<HasContainer> hasContainers, final List<HasContainer> adjustedIdContainers) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, hasContainers, adjustedIdContainers);
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedEdgeIds.
     */
    public Iterator<FireflyId> getSupernodeEdgeIds(final Direction direction, final Set<String> labels,
                                                   final List<HasContainer> hasContainers, final Map<FireflyId, FireflyEdge> edgeCache) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, hasContainers, edgeCache);
    }

    /**
     * Important note about this function: It only returns ids that are cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getSupernodeVertexIds.
     */
    public List<FireflyId> getCachedEdgeIds(final Direction direction, final Set<String> labels) {
        return getCachedIds(direction, labels).stream().map(id -> {
            if (id instanceof FireflyIdComposite) {
                return ((FireflyIdComposite) id).getEdgeId();
            } else {
                return id;
            }
        }).collect(Collectors.toList());
    }

    private List<FireflyId> getCachedEdgeIds(final Direction direction, final Set<String> labels, final List<HasContainer> adjustedIdContainers) {
        if (adjustedIdContainers == null) {
            return getCachedEdgeIds(direction, labels);
        }

        final List<FireflyId> result = new ArrayList<>();
        getCachedIds(direction, labels).forEach(id -> {
            if (id instanceof FireflyIdComposite
                    && adjustedIdContainers.stream().allMatch(c -> c.test(new ReferenceVertex(((FireflyIdComposite) id).getAdjacentId())))) {
                result.add(((FireflyIdComposite) id).getEdgeId());
            }
        });

        return result;
    }

    /**
     * Important note about this function: It only returns ids that are NOT cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getCachedVertexIds.
     */
    public Iterator<FireflyId> getSupernodeVertexIds(final Direction direction, final Set<String> labels) {
        return getSupernodeIds(direction, labels, FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID, Collections.emptyList());
    }

    /**
     * Important note about this function: It only returns ids that are cached in the vertex.
     * To get an exhaustive list of all ids you must call this in conjunction with getSupernodeVertexIds.
     */
    private List<FireflyId> getCachedVertexIds(final Direction direction, final Set<String> labels) {
        return getCachedIds(direction, labels).stream().map(id -> ((FireflyIdComposite) id).getAdjacentId()).collect(Collectors.toList());
    }

    public Iterator<FireflyId> getSupernodeIds(final Direction direction,
                                               final Set<String> labels,
                                               final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                               final List<HasContainer> hasContainers) {
        return getSupernodeIds(direction, labels, outputType, hasContainers, (List<HasContainer>) null);
    }

    public Iterator<FireflyId> getSupernodeIds(final Direction direction,
                                               final Set<String> labels,
                                               final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                               final List<HasContainer> hasContainers,
                                               final List<HasContainer> adjustedIdContainers) {
        if (!this.isEdgeCacheOverflowed) {
            return Collections.emptyIterator();
        }

        // todo: add filtering for otherVertexIds (!!!)

        LOG.trace("Getting supernode edge ids from vertex {}.", id);
        return getIdsFromVertexByIndex(direction, labels, outputType, hasContainers, adjustedIdContainers);
    }

    public Iterator<FireflyId> getSupernodeIds(final Direction direction,
                                               final Set<String> labels,
                                               final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                               final List<HasContainer> hasContainers, final Map<FireflyId, FireflyEdge> edgeCache) {
        if (!this.isEdgeCacheOverflowed) {
            return Collections.emptyIterator();
        }

        LOG.trace("Getting supernode edge ids from vertex {}.", id);
        return getIdsFromVertexByIndex(direction, labels, outputType, hasContainers, edgeCache);
    }

    public List<FireflyId> getCachedIds(final Direction direction, final Set<String> labels) {
        LOG.trace("Getting cached adjacent vertex ids from vertex {}.", id);
        // Get cached IDs
        final List<FireflyId> cachedIds = new ArrayList<>();
        if (direction == Direction.OUT || direction == Direction.BOTH) {
            for (final String key : outEdgeIds.keySet()) {
                if (labels.isEmpty() || labels.contains(key)) {
                    cachedIds.addAll(outEdgeIds.get(key).stream().map(LazyIdTransform::transform).collect(Collectors.toList()));
                }
            }
        }
        if (direction == Direction.IN || direction == Direction.BOTH) {
            for (final String key : inEdgeIds.keySet()) {
                if (labels.isEmpty() || labels.contains(key)) {
                    cachedIds.addAll(inEdgeIds.get(key).stream().map(LazyIdTransform::transform).collect(Collectors.toList()));
                }
            }
        }
        return cachedIds;
    }

    /**
     * Get vertices in a specified direction. This function leverages composite ids when appropriate.
     *
     * @param direction  Direction to get edge ids for.
     * @param edgeLabels Edge labels.
     * @return Iterator of vertices.
     */
    public Iterator<Vertex> getVerticesFromVertex(final Direction direction, final Set<String> edgeLabels) {
        LOG.trace("Getting vertices from vertex {}.", id);
        final Iterator<FireflyId> adjacentVertices = getVertexIdsFromVertex(direction, edgeLabels);
        return new FireflyBatchElementIterator<>(this.graph, adjacentVertices, Collections.emptyList(), this.graph::readVertices, null);
    }

    /**
     * Read vertex property keys.
     *
     * @return Set of vertex property keys.
     */
    protected Set<String> readVertexPropertyKeys() {
        return this.vertexProperties.keySet()
                .stream()
                .map(this.db.schemaManager::getVertexPropertyString)
                .collect(Collectors.toSet());
    }


    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }
        if (FireflyHelper.inComputerMode(this.graph)) {
            final List<VertexProperty> list = (List) this.graph.graphComputerView.getProperty(this, key);
            if (list.isEmpty()) {
                return VertexProperty.empty();
            } else if (list.size() == 1) {
                return list.get(0);
            } else {
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            }
        } else {
            final Iterator<VertexProperty<V>> iterator = this.readVertexProperty(key);
            if (!iterator.hasNext())
                return VertexProperty.empty();
            final VertexProperty<V> vp = iterator.next();
            if (iterator.hasNext())
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            return vp;
        }
    }

    /**
     * Create a new vertex property. If the cardinality is {@link VertexProperty.Cardinality#single}, then set the key
     * to the value. If the cardinality is {@link VertexProperty.Cardinality#list}, then add a new value to the key.
     * If the cardinality is {@link VertexProperty.Cardinality#set}, then only add a new value if that value doesn't
     * already exist for the key. If the value already exists for the key, add the provided key value vertex property
     * properties to it.
     *
     * @param cardinality the desired cardinality of the property key
     * @param key         the key of the vertex property
     * @param value       The value of the vertex property
     * @param keyValues   the key/value pairs to turn into vertex property properties
     * @param <V>         the type of the value of the vertex property
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality,
                                          final String key,
                                          final V value,
                                          final Object... keyValues) {
        if (cardinality.equals(VertexProperty.Cardinality.set)) {
            throw new AerospikeGraphException(GraphError.SET_CARDINALITY_NOT_SUPPORTED);
        }

        if (FireflyHelper.inComputerMode(this.graph)) {
            final VertexProperty<V> vertexProperty = (VertexProperty<V>) this.graph.graphComputerView.addProperty(this, key, value);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            return vertexProperty;
        }

        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }

        if (SUPERNODE_PROPERTY_KEY.equals(key)) {
            graph.aerospikeOperations.setCacheDisabled(this);
            return VertexProperty.empty();
        }

        // Handle TTL.
        if (TTL_PROPERTY_KEY.equals(key)) {
            if (!db.TTL_ENABLED_FLAG) {
                throw new AerospikeGraphException(GraphError.TTL_NOT_ENABLED);
            }
            if (value == null) {
                return VertexProperty.empty();
            }
            if (Number.class.isAssignableFrom(value.getClass())) {
                graph.aerospikeOperations.setTtl(this, ((Number) value).longValue());
                return VertexProperty.empty();
            } else {
                throw new TtlArgumentException(value);
            }
        }

        // Validate key.
        ElementHelper.validateProperty(key, value);

        // If the value is null, return empty when cardinality is set or list. If single, handle later by removing key.
        if (null == value && VertexProperty.Cardinality.single != cardinality) {
            return VertexProperty.empty();
        }

        // Verify no user-provided IDs are in the meta-properties.
        if (ElementHelper.getIdValue(keyValues).isPresent()) {
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }
        // Process meta-properties.
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final Map<Long, List<Object>> properties = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            final String propertyKey = keyValues[i] instanceof T ? ((T) keyValues[i]).getAccessor() : (String) keyValues[i];
            final Object propertyValue = keyValues[i + 1];
            ElementHelper.validateProperty(propertyKey, propertyValue);
            final Long schemaPropertyKey = db.schemaManager.getVpPropertyWrite(propertyKey);

            if (properties.containsKey(schemaPropertyKey) && propertyValue == null) {
                properties.remove(schemaPropertyKey);
            } else if (propertyValue != null) {
                final List<Object> valueAndTypeHint = new ArrayList<>(2);
                valueAndTypeHint.add(propertyValue);
                valueAndTypeHint.add(getTypeHintOf(propertyValue));
                properties.put(schemaPropertyKey, valueAndTypeHint);
            }
        }

        // Write vertex property to graph.
        final VertexProperty<V> vertexProperty = graph.aerospikeOperations.writeVertexProperty(cardinality, this,
                key, value, properties);

        // Return vertex property.
        return vertexProperty;
    }

    @Override
    public Set<String> keys() {
        return FireflyHelper.inComputerMode((FireflyGraph) graph()) ?
                Vertex.super.keys() : readVertexPropertyKeys();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        FireflyHelper.legalPropertyKeyValueArray(keyValues);

        // Validate edge and vertex.
        if (ElementHelper.getIdValue(keyValues).isPresent() && !graph.features().edge().supportsUserSuppliedIds())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (null == vertex)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (null == label || label.isEmpty())
            throw Graph.Exceptions.argumentCanNotBeNull("label");
        if (isHidden(label))
            throw Edge.Exceptions.labelCanNotBeAHiddenKey(label);
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }

        // Get id for edge.
        final FireflyEdgeId edgeId = (FireflyEdgeId) graph.getIdFactory().generateId(graph, FireflyEdge.class);

        // Write fully qualified edge.
        final List<Map.Entry<String, Object>> properties =
                graph.convertFullyQualified(graph.features().edge().supportsNullPropertyValues(), keyValues);
        return graph.getAerospikeOperations().writeEdge(edgeId, label, properties, (FireflyVertex) vertex, this);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }
        final Iterator<Edge> edgeIterator = FireflyHelper.getEdges(graph, this, direction, edgeLabels);
        return FireflyHelper.inComputerMode(this.graph) ?
                FireflyCloseableIteratorUtils.filter(edgeIterator,
                        edge -> this.graph.graphComputerView.legalEdge(this, edge)) :
                edgeIterator;
    }

    protected Iterator<FireflyId> getIdsFromVertexByIndex(final Direction direction,
                                                          final Set<String> labels,
                                                          final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                          final List<HasContainer> hasContainers,
                                                          final List<HasContainer> adjustedIdContainers) {
        if (direction == Direction.BOTH) {
            final Iterator<KeyRecord> inKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.IN, labels, outputType, hasContainers);
            final Iterator<KeyRecord> outKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.OUT, labels, outputType, hasContainers);
            return FireflyCloseableIteratorUtils.concat(
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(inKeyRecordIterator, this.db, Direction.IN, this.id, labels, outputType, adjustedIdContainers),
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(outKeyRecordIterator, this.db, Direction.OUT, this.id, labels, outputType, adjustedIdContainers));
        } else {
            final Iterator<KeyRecord> keyRecordIterator = getEdgeKeyRecordsByIndex(direction, labels, outputType, hasContainers);
            return new FireflyPhatEdgeIdIteratorFromIndexedVertex(keyRecordIterator, this.db, direction, this.id, labels, outputType, adjustedIdContainers);
        }
    }

    protected Iterator<FireflyId> getIdsFromVertexByIndex(final Direction direction,
                                                          final Set<String> labels,
                                                          final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                          final List<HasContainer> hasContainers,
                                                          final Map<FireflyId, FireflyEdge> edgeCache) {
        if (direction == Direction.BOTH) {
            final Iterator<KeyRecord> inKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.IN, labels, outputType, hasContainers);
            final Iterator<KeyRecord> outKeyRecordIterator = getEdgeKeyRecordsByIndex(Direction.OUT, labels, outputType, hasContainers);
            return FireflyCloseableIteratorUtils.concat(new FireflyPhatEdgeIdIteratorFromIndexedVertex(inKeyRecordIterator, this.graph, Direction.IN, this.id, labels, outputType, null, edgeCache),
                    new FireflyPhatEdgeIdIteratorFromIndexedVertex(outKeyRecordIterator, this.graph, Direction.OUT, this.id, labels, outputType, null, edgeCache));
        } else {
            final Iterator<KeyRecord> keyRecordIterator = getEdgeKeyRecordsByIndex(direction, labels, outputType, hasContainers);
            return new FireflyPhatEdgeIdIteratorFromIndexedVertex(keyRecordIterator, this.graph, direction, this.id, labels, outputType, null, edgeCache);
        }
    }

    /**
     * Get the KeyRecord iterator for Edges attached to this Vertex. Public only for testing purposes.
     *
     * @param direction
     * @param labels
     * @param outputType
     * @param hasContainers
     * @return KeyRecord iterator for Edges attached to this Vertex.
     */
    public Iterator<KeyRecord> getEdgeKeyRecordsByIndex(final Direction direction,
                                                        final Set<String> labels,
                                                        final FireflyPhatEdgeIdIteratorFromVertex.OutputType outputType,
                                                        final List<HasContainer> hasContainers) {
        return graph.aerospikeOperations.getEdgeKeyRecordsByIndex(this.id, direction, labels, outputType, hasContainers, null);
    }

    public long getEdgeCount(final Direction direction) {
        return getEdgeCount(direction, null);
    }

    public long getEdgeCount(final Direction direction, final String[] edgeLabels) {
        return getEdgeCount(direction, edgeLabels, -1, Collections.emptyList());
    }

    public long getEdgeCount(final Direction direction, final String[] edgeLabels, final long limit, final List<HasContainer> hasContainers) {
        if (direction == Direction.BOTH) {
            long count = getEdgeCount(Direction.IN, edgeLabels, limit, hasContainers)
                    + getEdgeCount(Direction.OUT, edgeLabels, limit, hasContainers);
            if (limit != -1 && count >= limit)
                return limit;
            return count;
        }

        final long count = this.isEdgeCacheOverflowed
                ? getCachedEdgeCount(direction, edgeLabels, hasContainers) +
                FireflyCloseableIteratorUtils.count(getSupernodeEdgeIds(direction, edgeLabels == null ? Collections.emptySet() : Set.of(edgeLabels), hasContainers))
                : getCachedEdgeCount(direction, edgeLabels, hasContainers);
        if (limit != -1 && count >= limit)
            return limit;

        return count;
    }

    private long getCachedEdgeCount(final Direction direction, final String[] edgeLabels, final List<HasContainer> hasContainers) {
        final Map<String, List<LazyIdTransform>> edgeCache = direction == Direction.IN ? this.inEdgeIds : this.outEdgeIds;
        long size = 0;

        if (edgeLabels == null || edgeLabels.length == 0) {
            for (final List<LazyIdTransform> ids : edgeCache.values()) {
                size += countWithFilter(ids, hasContainers);
            }
        } else {
            for (final String edgeLabel : edgeLabels) {
                if (edgeCache.containsKey(edgeLabel))
                    size += countWithFilter(edgeCache.get(edgeLabel), hasContainers);
            }
        }
        return size;
    }

    private long countWithFilter(final List<LazyIdTransform> ids, final List<HasContainer> hasContainers) {
        if (hasContainers.isEmpty())
            return ids.size();

        // f for Firefly
        final List<P<FireflyId>> fPredicates = new ArrayList<>();
        for (HasContainer hasContainer : hasContainers) {
            final Object predicateValue = hasContainer.getPredicate().getValue();
            final Object fId;
            // for P.Within()
            if (predicateValue instanceof List) {
                fId = new ArrayList<>();
                for (final Object id : (List) predicateValue) {
                    ((List) fId).add(graph.getIdFactory().createVertexId(id));
                }
            } else
                fId = graph.getIdFactory().createVertexId(predicateValue);

            fPredicates.add(new P(hasContainer.getPredicate().getBiPredicate(), fId));
        }

        long count = 0;
        boolean ok;
        for (final LazyIdTransform id : ids) {
            ok = true;
            FireflyIdPoly fId = (FireflyIdPoly) ((FireflyIdComposite) id.transform()).getAdjacentId();
            for (final P<FireflyId> p : fPredicates) {
                if (!p.test(fId)) {
                    ok = false;
                    break;
                }
            }
            if (ok)
                count++;
        }
        return count;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }
        final Set<String> edgeLabelSet = new HashSet<>(Arrays.asList(edgeLabels));
        return getVerticesFromVertex(direction, edgeLabelSet);
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (this.removed) {
            throw elementAlreadyRemoved(Vertex.class, this.id);
        }
        if (propertyKeys == null) {
            return Collections.emptyIterator();
        }
        if (propertyKeys.length == 0) {
            final Set<String> propertyKeySet = readVertexPropertyKeys();
            Iterator<VertexProperty<V>> itty = Collections.emptyIterator();
            for (final String propertyKey : propertyKeySet) {
                final Iterator<VertexProperty<V>> vpItty = readVertexProperty(propertyKey);
                itty = FireflyCloseableIteratorUtils.concat(itty, vpItty);
            }
            return itty;
        }
        final List<VertexProperty<V>> vertexProperties = new ArrayList<>();
        for (final String propertyKey : propertyKeys) {
            final Iterator<VertexProperty<V>> vpItty = readVertexProperty(propertyKey);
            vpItty.forEachRemaining(vertexProperties::add);

        }
        final Iterator<VertexProperty<V>> iterator = vertexProperties.iterator();
        if (!FireflyHelper.inComputerMode(this.graph)) {
            return iterator;
        } else {
            // TODO: GRAPH COMPUTER INTERCEPTION
            final LocalGraphComputerView view = FireflyHelper.getGraphComputerView(this.graph);
            final List<VertexProperty<V>> computeProperties = view.getComputeProperties(this, propertyKeys);
            return (computeProperties.isEmpty()) ? iterator : FireflyCloseableIteratorUtils.concat(iterator, computeProperties.iterator());
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    /**
     * Return whether this vertex's edge cache was filled and therefore potentially has edges in the edge set in
     * addition to those currently in the cache.
     *
     * @return is the edge cache overflowed.
     */
    public boolean isEdgeCacheOverflowed() {
        return this.isEdgeCacheOverflowed;
    }

    public void setIsEdgeCacheOverflowed(final boolean isEdgeCacheOverflowed) {
        this.isEdgeCacheOverflowed = isEdgeCacheOverflowed;
    }
}
