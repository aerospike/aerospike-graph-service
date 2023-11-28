package com.aerospike.firefly.sizing;

import com.aerospike.firefly.schema.EdgeSchema;
import com.aerospike.firefly.schema.GraphSchema;
import com.aerospike.firefly.schema.PropertySchema;
import com.aerospike.firefly.schema.VertexSchema;
import org.json.JSONObject;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class SizingTool {
    private final GraphSchema graphSchema;

    // Need to check all these.
    private final static Long EDGE_CACHE_ENTRY_SIZE = 36L;
    private final static Long EDGE_CACHE_ENTRY_OVERHEAD = 4L;
    private final static Long EDGE_RECORD_OVERHEAD = 20L;
    private final static Long EDGE_PROPERTY_OVERHEAD = 20L;
    private final static Long LABEL_OVERHEAD = 4L;
    private final static Long EDGE_RECORD_VALUE_SIZE = 43L;
    private final static Long EDGE_RECORD_ENTRY_OVERHEAD = 20L; // 16 bytes for the key which is the edge id.
                                                                // 4  bytes for the overhead of the map entry.

    private Long vertexRecordCount = 0L;
    private Long edgeRecordCount = 0L;
    private Double edgeCountPerVertex = 0.0;


    public SizingTool(final GraphSchema graphSchema) {
        this.graphSchema = graphSchema;
    }

    private static Long vertexPropertyToSize(final PropertySchema propertySchema) {
        // Property adds:   1 long typehint.
        //                  String of length key.
        //                  actual Value
        //                  1 map entry of empty map for properties of vertex property values.
        //                  1 map entry of empty map for properties of vertex properties type hints
        long size = 0L;
        // size += 5; // overhead
        size += propertySchema.key.length() * 3L; // vp name -> {type hint, value, id}
        size += propertyTypeToSize(propertySchema);
        size += 6; // 1 map entry of empty map for properties of vertex property.
        size += 6; // Empty map.
        size += 6; // 1 map entry of empty map
        size += 6; // Empty map.
        size += 10; // vp id.
        return size;
    }

    private static Long edgePropertyToSize(final PropertySchema propertySchema) {
        // Property adds:   1 long typehint.
        //                  String of length key.
        //                  actual Value
        //                  These are both stored in a map with edge id as key (adds 20 bytes overhead * 2)
        long size = 0L;
        size += 2 * EDGE_RECORD_ENTRY_OVERHEAD;
        size += 8;
        size += propertySchema.key.length();
        size += propertyTypeToSize(propertySchema);
        return size;
    }

    private static Long propertyTypeToSize(final PropertySchema propertySchema) {
        switch (propertySchema.type.toLowerCase()) {
            case "integer":
            case "float":
                return 4L;
            case "long":
            case "double":
                return 8L;
            case "string":
                if (propertySchema.size == null || propertySchema.size.longValue() <= 0L) {
                    throw new RuntimeException("Invalid property " + propertySchema.key + " type 'String' " +
                            "requires size set > 0. Got " + propertySchema.size + ".");
                }
                return propertySchema.size.longValue();
            case "byte[]":
                if (propertySchema.size == null || propertySchema.size.longValue() <= 0L) {
                    throw new RuntimeException("Invalid property " + propertySchema.key + " type 'byte[]' " +
                            "requires size set > 0. Got " + propertySchema.size + ".");
                }
                return propertySchema.size.longValue();
            default:
                throw new RuntimeException("Invalid property " + propertySchema.key + " type '" + propertySchema.type +
                        "' is not supported.");
        }
    }

    public long estimateVertexRecordCount() {
        if (this.vertexRecordCount == 0L) {
            for (final VertexSchema vertexSchema : graphSchema.vertexSchema) {
                vertexRecordCount += vertexSchema.count.longValue();
            }
        }
        return this.vertexRecordCount;
    }

    public long estimateEdgeRecordCount() {
        long edgeCount = 0L;
        if (this.edgeRecordCount == 0L) {
            for (final EdgeSchema edgeSchema : graphSchema.edgeSchema) {
                edgeCount += edgeSchema.count.longValue();
            }
            edgeRecordCount = edgeCount / graphSchema.edgePackSize.longValue();

            if (edgeCount % graphSchema.edgePackSize.longValue() != 0) {
                edgeRecordCount += 1;
            }
        }
        return this.edgeRecordCount;
    }

    private double getEdgeCacheEntrySize() {
        if (graphSchema.edgeSchema.isEmpty()) {
            return 0;
        }
        final long averageEdgeLabelSize = graphSchema.edgeSchema.stream().
                mapToLong(edgeSchema -> edgeSchema.label.length()).sum() / graphSchema.edgeSchema.size();
        return EDGE_CACHE_ENTRY_SIZE + EDGE_CACHE_ENTRY_OVERHEAD + averageEdgeLabelSize;
    }

    private double getEdgeCountPerVertex() {
        if (this.edgeCountPerVertex == 0) {
            long edgeCount = 0;
            for (final EdgeSchema edgeSchema : graphSchema.edgeSchema) {
                edgeCount += edgeSchema.count.longValue();
            }
            long vertexCount = 0;
            for (final VertexSchema vertexSchema : graphSchema.vertexSchema) {
                vertexCount += vertexSchema.count.longValue();
            }

            // Two cache entries per edge.
            // edgePackSize per edgeRecord.
            this.edgeCountPerVertex = 2 * (double) edgeCount / (double) vertexCount;
        }
        return this.edgeCountPerVertex;
    }

    public long estimateAverageVertexRecordSize() {
        double vertexRecordSize = 0L;
        for (final VertexSchema vertexSchema : graphSchema.vertexSchema) {
            vertexRecordSize += vertexSchema.label.length() + LABEL_OVERHEAD;

            //GRAPH_VARIABLES_BIN(Pair.of((byte) 1, "GRAPH_VARS")),
            //        VERTEX_PROPERTY_NAME_TO_VALUE_BIN(Pair.of((byte) 2, "VP_NAME_VAL")),
            //        VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN(Pair.of((byte) 3, "VP_HINT")),
            //        RELATIONAL_VERTEX_TYPE_HINT_BIN(Pair.of((byte) 4, "REL_VP_HINT")),
            //        IN_EDGES_BIN(Pair.of((byte) 7, "IN_EDGES")),
            //        OUT_EDGES_BIN(Pair.of((byte) 8, "OUT_EDGES")),
            //        PROPERTIES_BIN(Pair.of((byte) 9, "PROPERTIES")),
            //        TYPE_HINTS_BIN(Pair.of((byte) 10, "TYPE_HINTS")),
            //        COUNTER_BIN(Pair.of((byte) 11, "COUNTER")),
            //        ID_TYPE_BIN(Pair.of((byte) 12, "ID_TYPE")),
            //        USER_KEY_BIN(Pair.of((byte) 13, "USER_KEY")),
            //        LABEL_BIN(Pair.of((byte) 14, "LABEL")),
            //        IN_EDGE_COUNTER_BIN(Pair.of((byte) 15, "IN_E_C")),
            //        OUT_EDGE_COUNTER_BIN(Pair.of((byte) 16, "OUT_E_C")),
            //        VERTEX_PROPERTY_NAME_TO_ID_BIN(Pair.of((byte) 17, "VP_NAME_ID")),
            //vertexRecordSize += 8L; // user key avg
            //vertexRecordSize += 8L; // user id type
            //vertexRecordSize += 8L; // vertex type hint

            if (vertexSchema.properties.isEmpty()) {
                vertexRecordSize += 2 * 5L; // type hints and properties empty.
                vertexRecordSize += 4 * 5L; // vertex property k->v, type hint
            }

            for (final PropertySchema propertySchema : vertexSchema.properties) {
                vertexRecordSize += (propertySchema.likelihood.doubleValue()) * (vertexPropertyToSize(propertySchema));
            }
            vertexRecordSize += 10L; // empirical adjustment.

            double edgeCounterPerVertex = getEdgeCountPerVertex();
            if (graphSchema.edgeSchema.isEmpty() || edgeCounterPerVertex < 1.0) {
                vertexRecordSize += 2 * 15L; // two empty maps.
            }
            vertexRecordSize += (getEdgeCacheEntrySize() * getEdgeCountPerVertex());
        }

        vertexRecordSize /= graphSchema.vertexSchema.size();
        // Factor in edge cache average size
        return (long) vertexRecordSize;
    }

    public long estimateAverageEdgeRecordSize() {
        double edgeRecordSize = 0.0;
        for (final EdgeSchema edgeSchema : graphSchema.edgeSchema) {
            edgeRecordSize += EDGE_RECORD_OVERHEAD;
            if (edgeSchema.properties != null) {
                for (final PropertySchema propertySchema : edgeSchema.properties) {
                    edgeRecordSize += (propertySchema.likelihood.doubleValue()) * (edgePropertyToSize(propertySchema)); // Properties.
                }
                edgeRecordSize += 2 * 10L; // properties and type hitns overhead.

                // label is repeated for: properties, type hints, in, out.
                edgeRecordSize += 4 * edgeSchema.label.length();
                edgeRecordSize += LABEL_OVERHEAD; // Label.
                edgeRecordSize += 2 * (EDGE_RECORD_VALUE_SIZE); // IN and OUT.
                edgeRecordSize += 10; // empirical adjustment.
            }
        }
        edgeRecordSize /= graphSchema.edgeSchema.size();

        // Factor in edge cache average size
        return (long) edgeRecordSize * graphSchema.edgePackSize.longValue(); // 10 edges per record
    }

    // TODO: Some of this is implemented, need to finish it and uncomment.
    //public long largestEdgeRecordSize() {
    //    double largestEdgeRecord = 0.0;
    //    for (final EdgeSchema edgeSchema : graphSchema.edgeSchema) {
    //        double localLargestEdgeRecord = 0.0;
    //        localLargestEdgeRecord += EDGE_RECORD_OVERHEAD;
    //        if (edgeSchema.properties != null) {
    //            for (final PropertySchema propertySchema : edgeSchema.properties) {
    //                localLargestEdgeRecord += EDGE_PROPERTY_OVERHEAD;
    //                // Inserting 1.0 for likelihood, this is the worst case (in terms of size).
    //                // This ignores variance in property size...
    //                localLargestEdgeRecord += (1.0) * (propertyTypeToSize(propertySchema));
    //            }
    //        }
    //        largestEdgeRecord = Math.max(largestEdgeRecord, localLargestEdgeRecord);
    //    }

    //    // Factor in edge cache average size
    //    return (long) largestEdgeRecord * 10; // 10 edges per record
    //}

    public long largestVertexRecordSize() {
        double largestVertexRecord = 0L;
        for (final VertexSchema vertexSchema : graphSchema.vertexSchema) {
            double localLargestVertexRecord = 0.0;
            localLargestVertexRecord += vertexSchema.label.length() + LABEL_OVERHEAD;
            for (final PropertySchema propertySchema : vertexSchema.properties) {
                // Inserting 1.0 for likelihood, this is the worst case (in terms of size).
                // This ignores variance in property size...
                localLargestVertexRecord += (1.0) * (propertyTypeToSize(propertySchema));
            }
            // Assume max cache
            localLargestVertexRecord += (graphSchema.maxEdgeCacheSize.longValue() * getEdgeCacheEntrySize());

            largestVertexRecord = Math.max(largestVertexRecord, localLargestVertexRecord);
        }

        return (long) largestVertexRecord;
    }

    public long totalSindexEntries() {
        double sindexEntries = 0L;
        if (graphSchema.vertexLabelSindex) {
            // 1 sindex entry per
            for (final VertexSchema vertexSchema : graphSchema.vertexSchema) {
                sindexEntries += vertexSchema.count.longValue();
            }
        }

        for (final VertexSchema vertexSchema : graphSchema.vertexSchema) {
            for (final PropertySchema propertySchema : vertexSchema.properties) {
                // 1 entry per sindexed property, i.e likelihood * count
                if (propertySchema.sindexed) {
                    sindexEntries += (propertySchema.likelihood.doubleValue() * vertexSchema.count.longValue());
                }
            }
        }

        for (final EdgeSchema edgeSchema : graphSchema.edgeSchema) {
            for (final PropertySchema propertySchema : edgeSchema.properties) {
                // Not supported.
                if (propertySchema.sindexed) {
                    throw new RuntimeException("Cannot create sindex on edge with label '" + edgeSchema.label + "' and " +
                            " property '" + propertySchema.key + "'. Sindexing on edge properties is not supported at this time.");
                }
            }
        }
        return (long) sindexEntries;
    }

    public void formatToFile(final String outputPathAbsolute) {
        if (outputPathAbsolute.endsWith(".json")) {
            formatToJsonFile(outputPathAbsolute);
        } else if (outputPathAbsolute.endsWith(".csv")) {
            formatToCsvFile(outputPathAbsolute);
        } else if (outputPathAbsolute.endsWith(".yaml")) {
            formatToYamlFile(outputPathAbsolute);
        } else {
            throw new RuntimeException("Invalid output file type: " + outputPathAbsolute + ". File should end with '.yaml', '.json', or '.csv'.");
        }
    }

    private void formatToJsonFile(final String outputPathAbsolute) {
        final JSONObject root = new JSONObject();
        root.put("vertexRecordCount", estimateVertexRecordCount());
        root.put("edgeRecordCount", estimateEdgeRecordCount());
        root.put("averageVertexRecordSize", estimateAverageVertexRecordSize());
        root.put("averageEdgeRecordSize", estimateAverageEdgeRecordSize());
        root.put("totalSindexEntries", totalSindexEntries());
        try (final FileWriter file = new FileWriter(outputPathAbsolute)) {
            file.write(root.toString());
        } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public final Map<String, Long> asMap() {
        final Map<String, Long> map = new HashMap<>();
        map.put("vertexRecordCount", estimateVertexRecordCount());
        map.put("edgeRecordCount", estimateEdgeRecordCount());
        map.put("averageVertexRecordSize", estimateAverageVertexRecordSize());
        map.put("averageEdgeRecordSize", estimateAverageEdgeRecordSize());
        map.put("totalSindexEntries", totalSindexEntries());
        return map;
    }

    private void formatToYamlFile(final String outputPathAbsolute) {
        final Map<String, Long> map = asMap();

        // 'DumperOptions'
        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);

        final Yaml yaml = new Yaml(options);
        final String yamified = yaml.dump(map);

        try (final FileWriter file = new FileWriter(outputPathAbsolute)) {
            file.write(yamified);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void formatToCsvFile(final String outputPathAbsolute) {
        // TODO
        try (final FileWriter file = new FileWriter(outputPathAbsolute)) {
            file.write("T,O,D,O");
        } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // TODO: Once the sizing tool is accurate we can remove this, this is just for reference.
    // Sizing tool notes:
    //
    // MISC:
    //            GRAPH_VARIABLES_BIN(Pair.of((byte) 1, "GRAPH_VARS")),
    //            VERTEX_PROPERTY_NAME_TO_VALUE_BIN(Pair.of((byte) 2, "VP_NAME_VAL")),
    //            VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN(Pair.of((byte) 3, "VP_HINT")),
    //            RELATIONAL_VERTEX_TYPE_HINT_BIN(Pair.of((byte) 4, "REL_VP_HINT")),
    //            EDGE_CACHE_DISABLED_BIN(Pair.of((byte) 6, "ECACHE_OFF")),
    //            IN_EDGES_BIN(Pair.of((byte) 7, "IN_EDGES")),
    //            OUT_EDGES_BIN(Pair.of((byte) 8, "OUT_EDGES")),
    //            PROPERTIES_BIN(Pair.of((byte) 9, "PROPERTIES")),
    //            TYPE_HINTS_BIN(Pair.of((byte) 10, "TYPE_HINTS")),
    //            COUNTER_BIN(Pair.of((byte) 11, "COUNTER")),
    //            ID_TYPE_BIN(Pair.of((byte) 12, "ID_TYPE")),
    //            USER_KEY_BIN(Pair.of((byte) 13, "USER_KEY")),
    //            LABEL_BIN(Pair.of((byte) 14, "LABEL")),
    //            IN_EDGE_COUNTER_BIN(Pair.of((byte) 15, "IN_E_C")),
    //            OUT_EDGE_COUNTER_BIN(Pair.of((byte) 16, "OUT_E_C")),
    //            VERTEX_PROPERTY_NAME_TO_ID_BIN(Pair.of((byte) 17, "VP_NAME_ID"));
    //
    //
    // Basic empty vertex record:
    // bins:
    // (2:{}),          => VERTEX_PROPERTY_NAME_TO_VALUE_BIN = {}
    // (3:{}),          => VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT = {}
    // (4:1),           => RELATIONAL_VERTEX_TYPE_HINT = 1
    // (6:false),       => ECACHE_OFF = false
    // (7:{}),          => IN_EDGES = {}
    // (8:{}),          => OUT_EDGES = {}
    // (9:{}),          => PROPERTIES = {}
    // (10:{}),         => TYPE_HINTS = {}
    // (12:1),          => ID_TYPE = 1
    // (13:-1001)       => USER_KEY = -1001
    // (14:vertex),     => LABEL = vertex
    // (17:{}),         => VERTEX_PROPERTY_NAME_TO_ID = {}
    //
    // Basic vertex record with 1 property and label:
    // bins:
    // Property adds:   1 long typehint.
    //                  String of length key.
    //                  actual Value
    //                  1 map entry of empty map for properties of vertex property.
    //                  1 map entry of empty map
    // (2:{name=Lyndon})
    // (3:{name=5})
    // (4:1)
    // (6:false)
    // (7:{})
    // (8:{})
    // (9:{-1={}})
    // (10:{-1={}})
    // (12:1)
    // (13:-2001)
    // (14:Person)
    // (17:{name=-1})
}
