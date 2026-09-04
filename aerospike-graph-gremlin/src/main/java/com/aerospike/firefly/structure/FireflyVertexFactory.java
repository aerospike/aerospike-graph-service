/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.LazyEdgeCacheIdTransform;
import com.aerospike.firefly.structure.id.LazyIdTransform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FireflyVertexFactory {
    public static FireflyVertex create(final FireflyId fid,
                                       final String label,
                                       final FireflyGraph graph,
                                       final Map<String, List<LazyIdTransform>> inEdgeIds,
                                       final Map<String, List<LazyIdTransform>> outEdgeIds,
                                       final Map<Long, HashMap<Object, List<Long>>> vertexProperties,
                                       final Map<Long, Map<Long, Object>> vpTypeHints,
                                       final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties,
                                       final Map<Long, List<String>> geoData,
                                       final boolean isEdgeCacheOverflowed) {

        return new FireflyVertex(
                fid,
                label,
                graph,
                inEdgeIds,
                outEdgeIds,
                vertexProperties,
                vpTypeHints,
                vpProperties,
                geoData,
                isEdgeCacheOverflowed);
    }

    public static FireflyVertex create(final FireflyId fid,
                                       final String label,
                                       final FireflyGraph graph,
                                       final Map<String, List<LazyIdTransform>> inEdgeIds,
                                       final Map<String, List<LazyIdTransform>> outEdgeIds,
                                       final Map<Long, HashMap<Object, List<Long>>> vertexProperties,
                                       final Map<Long, Map<Long, Object>> vpTypeHints,
                                       final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties,
                                       final boolean isEdgeCacheOverflowed) {
        return create(fid, label, graph, inEdgeIds, outEdgeIds, vertexProperties, vpTypeHints, vpProperties,
                null, isEdgeCacheOverflowed);
    }

    /**
     * Construct vertex from KeyRecord.
     *
     * @param keyRecord KeyRecord to construct vertex with.
     * @param graph     FireflyGraph
     * @return FireflyVertex.
     */
    public static FireflyVertex create(final KeyRecord keyRecord, final FireflyGraph graph) {
        if (keyRecord == null) {
            return null;
        }

        final Record record = keyRecord.record;
        final AerospikeConnection db = graph.getBaseGraph();

        // Read the Vertex's record from the database.
        if (record == null) {
            return null;
        }

        // Get ID and Label for Vertex.
        final FireflyId id = graph.getIdFactory().createVertexIdFromRecord(keyRecord);
        final String label = graph.getBaseGraph().schemaManager.getVertexLabelString(record.getLong(db.getConfig().labelBin));

        // Get cache state.
        final boolean edgeCacheOverflowed = record.getBoolean(db.getConfig().edgeCacheDisabledBin);

        // Get inEdgeIds and outEdgeIds.
        final Map<String, List<Object>> inEdgeIds = (Map) record.getMap(db.getConfig().inEdgesBin);
        graph.getIdFactory().convertMapToLazyIdsInPlace(inEdgeIds, graph, LazyEdgeCacheIdTransform.class);
        final Map<String, List<Object>> outEdgeIds = (Map) record.getMap(db.getConfig().outEdgesBin);
        graph.getIdFactory().convertMapToLazyIdsInPlace(outEdgeIds, graph, LazyEdgeCacheIdTransform.class);
        final Map<String, List<LazyIdTransform>> fireflyInEdgeIds = (Map) inEdgeIds;
        final Map<String, List<LazyIdTransform>> fireflyOutEdgeIds = (Map) outEdgeIds;

        // Get Vertex Properties and VP Properties
        final Map<Long, HashMap<Object, List<Long>>> vertexProperties =
                (Map<Long, HashMap<Object, List<Long>>>) record.getMap(db.getConfig().vertexPropertyDataBin);
        final Map<Long, Map<Long, Object>> vpTypeHints =
                (Map<Long, Map<Long, Object>>) record.getMap(db.getConfig().vertexPropertyTHBin);
        final Map<Long, Map<Long, Map<Long, List<Object>>>> vpProperties = (Map) record.getMap(db.getConfig().vpPropertyBin);
        final Map<Long, List<String>> geoData = readGeoDataMap(record, db.getConfig().geoDataBin);

        return create(id, label, graph, fireflyInEdgeIds, fireflyOutEdgeIds,
                vertexProperties, vpTypeHints, vpProperties, geoData, edgeCacheOverflowed);
    }

    static Map<Long, List<String>> normalizeGeoDataMap(final Map<Long, List<String>> geoData) {
        if (geoData == null || geoData.isEmpty()) {
            return new TreeMap<>();
        }
        final Map<Long, List<String>> normalized = new TreeMap<>();
        for (final Map.Entry<Long, List<String>> entry : geoData.entrySet()) {
            final Long schemaKey = entry.getKey() instanceof Number
                    ? ((Number) entry.getKey()).longValue()
                    : entry.getKey();
            normalized.put(schemaKey, entry.getValue());
        }
        return normalized;
    }

    private static Map<Long, List<String>> readGeoDataMap(final Record record, final String binName) {
        if (record == null || !record.bins.containsKey(binName)) {
            return null;
        }
        final Map<?, ?> raw = record.getMap(binName);
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        final Map<Long, List<String>> geoData = new TreeMap<>();
        for (final Map.Entry<?, ?> entry : raw.entrySet()) {
            final Long schemaKey = entry.getKey() instanceof Number
                    ? ((Number) entry.getKey()).longValue()
                    : (Long) entry.getKey();
            @SuppressWarnings("unchecked")
            final List<String> points = (List<String>) entry.getValue();
            geoData.put(schemaKey, points);
        }
        return geoData;
    }
}
