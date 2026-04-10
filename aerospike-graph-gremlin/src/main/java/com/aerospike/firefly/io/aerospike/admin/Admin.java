package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;

public class Admin {
    public static final Index INDEX = new Index();
    private static final String VERTEX_LABEL_TOKEN = "~vertex:LABEL";

    public static class Index<I> {
        public I getIndexList(final FireflyGraph firefly) {
            try {
                // Manually force an update.
                firefly.fireflyIndexMetadata.updateMetadata();
                firefly.fireflyCardinalityMetadata.updateMetadata();
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to update index information. " + e.getMessage());
            }

            final List<String> vertexPropertyStringIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes(STRING);

            final List<String> validVertexPropertyIndexes = new ArrayList<>();
            for (final String index : vertexPropertyStringIndexes) {
                try {
                    final Map<String, Long> indexInfo = (Map<String, Long>) getStatusVertexPropertyIndex(firefly, index, STRING);
                    if (indexInfo.get("percent_complete") == 100L) {
                        validVertexPropertyIndexes.add(index + ":STRING");
                    }
                } catch (final IllegalStateException ignored) {
                    // Do nothing.
                }
            }

            final List<String> vertexPropertyNumericIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes(NUMERIC);

            for (final String index : vertexPropertyNumericIndexes) {
                try {
                    final Map<String, Long> indexInfo = (Map<String, Long>) getStatusVertexPropertyIndex(firefly, index, NUMERIC);
                    if (indexInfo.get("percent_complete") == 100L) {
                        validVertexPropertyIndexes.add(index + ":NUMERIC");
                    }
                } catch (final IllegalStateException ignored) {
                    // Do nothing.
                }
            }

            final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();
            try {
                if (vertexLabelIndex && getIndexStatus(firefly, firefly.getBaseGraph().getConfig().vLabelIndexName).get("percent_complete") == 100L) {
                    validVertexPropertyIndexes.add(VERTEX_LABEL_TOKEN);
                }
            } catch (final IllegalStateException ignored) {
                // Do nothing.
            }

            // Return as a list so it can be used programatically.
            return (I) validVertexPropertyIndexes;
        }

        public I getExpressionIndexList(final FireflyGraph firefly) {
            try {
                firefly.fireflyIndexMetadata.updateMetadata();
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to update index information. " + e.getMessage());
            }
            return (I) firefly.fireflyIndexMetadata.getExpressionIndexNames();
        }

        public I dropExpressionIndex(final FireflyGraph firefly, final String indexName) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set, indexName);
            return (I) ("Compound index '" + indexName + "' dropped.");
        }

        public I getExpressionIndexStatus(final FireflyGraph firefly, final String indexName) {
            try {
                return (I) getIndexStatus(firefly, indexName);
            } catch (final IllegalStateException e) {
                throw new IllegalStateException("Compound index not found: " + indexName);
            }
        }

        public I getIndexCardinality(final FireflyGraph firefly) {
            try {
                // Manually force an update.
                firefly.fireflyIndexMetadata.updateMetadata();
                firefly.fireflyCardinalityMetadata.updateMetadata();
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to update index information. " + e.getMessage());
            }

            final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();

            final Map<String, Long> cardinalityMap = new HashMap<>();
            if (vertexLabelIndex) {
                firefly.fireflyCardinalityMetadata.getVertexLabelCardinality().ifPresent(cardinality -> {
                    Long cardinalityValue = cardinality.getCardinality();
                    if (cardinalityValue != null) {
                        cardinalityMap.put(VERTEX_LABEL_TOKEN, cardinalityValue);
                    }
                });
            }
            final List<String> vertexPropertyStringIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes(STRING);
            for (final String index : vertexPropertyStringIndexes) {
                firefly.fireflyCardinalityMetadata.getVertexPropertyCardinality(index, STRING).ifPresent(cardinality -> {
                    final Long cardinalityValue = cardinality.getCardinality();
                    if (cardinalityValue != null) {
                        cardinalityMap.put(index + ":STRING", cardinalityValue);
                    }
                });
            }
            final List<String> vertexPropertyNumericIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes(NUMERIC);
            for (final String index : vertexPropertyNumericIndexes) {
                firefly.fireflyCardinalityMetadata.getVertexPropertyCardinality(index, NUMERIC).ifPresent(cardinality -> {
                    final Long cardinalityValue = cardinality.getCardinality();
                    if (cardinalityValue != null) {
                        cardinalityMap.put(index + ":NUMERIC", cardinalityValue);
                    }
                });
            }
            return (I) cardinalityMap;
        }

        public I createVertexPropertyIndex(final FireflyGraph firefly, final String key, final IndexType indexType) {
            firefly.createIndexes(FireflyVertex.class,
                    firefly.getBaseGraph().getConfig().vertexPropertyDataBin,
                    firefly.getBaseGraph().getVpIndexPrefix(),
                    List.of(key),
                    indexType,
                    true);
            return (I) ("Vertex index creation of type '" + indexType + "' on property key '" + key + "' in progress.");
        }

        public I dropVertexPropertyIndex(final FireflyGraph firefly, final String key, final IndexType indexType) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set,
                    String.format("%s_%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key, indexType));
            return (I) ("Vertex index of type '" + indexType + "' on property key '" + key + "' dropped.");
        }

        private static List<String> getExistingIndexes(final FireflyGraph firefly) {
            return AerospikeConnection.InfoOps.
                    listExistingIndexes(firefly.getBaseGraph()).
                    stream().map(Map.Entry::getKey).collect(Collectors.toList());
        }

        public I createVertexLabelIndex(final FireflyGraph firefly) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            final List<String> existingIndexes = getExistingIndexes(firefly);
            firefly.getBaseGraph().createIndexBackground(existingIndexes,
                    set,
                    firefly.getBaseGraph().getConfig().vLabelIndexName,
                    firefly.getBaseGraph().getConfig().labelBin,
                    IndexType.NUMERIC,
                    IndexCollectionType.DEFAULT,
                    true);
            return (I) "Vertex label index creation in progress.";
        }

        public I dropVertexLabelIndex(final FireflyGraph firefly) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set, firefly.getBaseGraph().getConfig().vLabelIndexName);
            return (I) "Vertex label index dropped.";
        }

        public I getStatusVertexLabelIndex(final FireflyGraph firefly) {
            try {
                return (I) getIndexStatus(firefly, firefly.getBaseGraph().getConfig().vLabelIndexName);
            } catch (final IllegalStateException e) {
                throw new IllegalStateException("No index found on vertex label.");
            }
        }

        public static Map<String, Long> getIndexStatus(final FireflyGraph firefly, final String indexName) {
            long lowestLoadPct = 100;
            long totalEntries = 0;
            long totalUsedBytes = 0;
            long highestLoadTime = 0;
            long highestMemoryUsed = 0;
            boolean valid = false;
            final List<String> infoResponses = AerospikeConnection.InfoOps.getIndexStatuses(firefly.getBaseGraph(), indexName);
            for (final String infoResponse : infoResponses) {
                for (String s : infoResponse.split(";")) {
                    valid = true;
                    if (s.startsWith("load_pct=")) {
                        final long loadPct = Long.parseLong(s.split("=")[1]);
                        lowestLoadPct = Math.min(loadPct, lowestLoadPct);
                    } else if (s.startsWith("entries=")) {
                        final long entries = Long.parseLong(s.split("=")[1]);
                        totalEntries += entries;
                    } else if (s.startsWith("used_bytes=")) {
                        final long usedBytes = Long.parseLong(s.split("=")[1]);
                        totalUsedBytes += usedBytes;
                    } else if (s.startsWith("load_time=")) {
                        final long loadTime = Long.parseLong(s.split("=")[1]);
                        highestLoadTime = Math.max(loadTime, highestLoadTime);
                    } else if (s.startsWith("memory_used=")) {
                        final long memoryUsed = Long.parseLong(s.split("=")[1]);
                        highestMemoryUsed = Math.max(memoryUsed, highestMemoryUsed);
                    }
                }
            }
            if (valid) {
                // This is the case if the index is dropped.
                if (totalUsedBytes != 0 || highestMemoryUsed != 0) {
                    return Map.of("percent_complete", lowestLoadPct,
                            "total_entries", totalEntries,
                            "total_used_bytes", totalUsedBytes,
                            "load_time", highestLoadTime);
                }
            }
            throw new IllegalStateException("Index not found: " + indexName + ".");
        }

        public I getStatusVertexPropertyIndex(final FireflyGraph firefly, final String key, final IndexType indexType) {
            final String formattedIndex = String.format("%s_%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key, indexType);
            try {
                final Map<String, Long> indexStatus = getIndexStatus(firefly, formattedIndex);
                return (I) Map.of("percent_complete", indexStatus.get("percent_complete"),
                        "total_entries", indexStatus.get("total_entries"),
                        "total_used_bytes", indexStatus.get("total_used_bytes"),
                        "load_time", indexStatus.get("load_time"));
            } catch (final IllegalStateException e) {
                throw new IllegalStateException("No '" + indexType + "' index found for vertex property key '" + key + "'.");
            }
        }

        public I getStatusVertexPropertyIndex(final FireflyGraph firefly, final String key) {
            Map<String, Long> stringIndexStatus = null;
            Map<String, Long> numericIndexStatus = null;
            try {
                stringIndexStatus = (Map<String, Long>) getStatusVertexPropertyIndex(firefly, key, STRING);
            } catch (final IllegalStateException ignored) {
                // Handled later
            }
            try {
                numericIndexStatus = (Map<String, Long>) getStatusVertexPropertyIndex(firefly, key, NUMERIC);
            } catch (final IllegalStateException ignored) {
                // Handled later
            }
            if (stringIndexStatus == null && numericIndexStatus == null) {
                throw new IllegalStateException("No index found for vertex property key '" + key + "'.");
            } else if (stringIndexStatus != null && numericIndexStatus != null) {
                final Map<String, Long> indexStatus = new HashMap<>(numericIndexStatus);
                stringIndexStatus.forEach((k, v) -> {
                    indexStatus.merge(k, v, (numV, strV) -> {
                        if (k.equals("percent_complete")) {
                            return Math.min(numV,strV);
                        } else if (k.equals("load_time")) {
                            return Math.max(numV, strV);
                        } else {
                            return numV + strV;
                        }
                    });
                });
                return (I) indexStatus;
            } else {
                return (I) (stringIndexStatus == null ? numericIndexStatus : stringIndexStatus);
            }
        }
    }
}
