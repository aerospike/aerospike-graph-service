package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;

public class Admin {
    private static final Logger LOGGER = LoggerFactory.getLogger(Admin.class);
    public static final Index index = new Index();

    public static class Index<I> {
        public I getIndexList(final FireflyGraph firefly) {
            try {
                // Manually force an update.
                firefly.fireflyIndexMetadata.updateMetadata();
                firefly.fireflyCardinalityMetadata.updateMetadata();
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to update index information. " + e.getMessage());
            }

            final List<String> vertexPropertyIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes();

            final List<String> validVertexPropertyIndexes = new ArrayList<>();
            for (final String index : vertexPropertyIndexes) {
                try {
                    final Map<String, Long> indexInfo = (Map<String, Long>) getStatusVertexPropertyIndex(firefly, index);
                    if (indexInfo.get("percent_complete") == 100L) {
                        validVertexPropertyIndexes.add(index);
                    }
                } catch (final IllegalStateException ignored) {
                    // Do nothing.
                }
            }

            final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();
            try {
                if (vertexLabelIndex && getIndexStatus(firefly, firefly.getBaseGraph().V_LABEL_INDEX_NAME).get("percent_complete") == 100L) {
                    validVertexPropertyIndexes.add("vertex.~label");
                }
            } catch (final IllegalStateException ignored) {
                // Do nothing.
            }

            // Return as a list so it can be used programatically.
            return (I) validVertexPropertyIndexes;
        }

        public I getIndexCardinality(final FireflyGraph firefly) {
            try {
                // Manually force an update.
                firefly.fireflyIndexMetadata.updateMetadata();
                firefly.fireflyCardinalityMetadata.updateMetadata();
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to update index information. " + e.getMessage());
            }

            final List<String> vertexPropertyIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes();
            final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();

            final Map<String, Long> cardinalityMap = new HashMap<>();
            if (vertexLabelIndex) {
                firefly.fireflyCardinalityMetadata.getVertexLabelCardinality().ifPresent(cardinality -> {
                    Long cardinalityValue = cardinality.getCardinality();
                    if (cardinalityValue != null) {
                        cardinalityMap.put("~vertex.label", cardinalityValue);
                    }
                });
            }
            for (final String index : vertexPropertyIndexes) {
                firefly.fireflyCardinalityMetadata.getVertexPropertyCardinality(index, STRING).ifPresent(cardinality -> {
                    final Long cardinalityValue = cardinality.getCardinality();
                    if (cardinalityValue != null) {
                        cardinalityMap.put(index, cardinalityValue);
                    }
                });
                firefly.fireflyCardinalityMetadata.getVertexPropertyCardinality(index, NUMERIC).ifPresent(cardinality -> {
                    final Long cardinalityValue = cardinality.getCardinality();
                    if (cardinalityValue != null) {
                        if (cardinalityMap.get(index) != null) {
                            cardinalityMap.put(index, cardinalityMap.get(index) + cardinalityValue);
                        }
                    }
                });
            }
            return (I) cardinalityMap;
        }

        public I createVertexPropertyIndex(final FireflyGraph firefly, final String key) {
            firefly.createIndexes(FireflyVertex.class, firefly.getBaseGraph().VERTEX_PROPERTY_DATA_BIN, firefly.getBaseGraph().getVpIndexPrefix(), List.of());
            return (I) ("Vertex index creation of property key '" + key + "' in progress.");
        }

        public I dropVertexPropertyIndex(final FireflyGraph firefly, final String key) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set,
                    String.format("%s_%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key, STRING));
            firefly.getBaseGraph().dropIndexBackground(set,
                    String.format("%s_%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key, NUMERIC));
            return (I) ("Vertex index of property key '" + key + "' dropped.");
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
                    firefly.getBaseGraph().V_LABEL_INDEX_NAME,
                    firefly.getBaseGraph().LABEL_BIN,
                    IndexType.NUMERIC,
                    IndexCollectionType.DEFAULT,
                    true);
            return (I) "Vertex label index creation in progress.";
        }

        public I dropVertexLabelIndex(final FireflyGraph firefly) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set, firefly.getBaseGraph().V_LABEL_INDEX_NAME);
            return (I) "Vertex label index dropped.";
        }

        public I getStatusVertexLabelIndex(final FireflyGraph firefly) {
            try {
                return (I) getIndexStatus(firefly, firefly.getBaseGraph().V_LABEL_INDEX_NAME);
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

        public I getStatusVertexPropertyIndex(final FireflyGraph firefly, final String key) {
            final String formattedIndex = String.format("%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key);
            final String stringIndexName = formattedIndex + "_" + STRING;
            final String numericIndexName = formattedIndex + "_" + NUMERIC;
            try {
                final Map<String, Long> numericIndexStatus = getIndexStatus(firefly, numericIndexName);
                final Map<String, Long> stringIndexStatus = getIndexStatus(firefly, stringIndexName);
                if (numericIndexStatus.get("percent_complete") == 100L && stringIndexStatus.get("percent_complete") == 100L) {
                    return (I) Map.of("percent_complete", (long) 100,
                            "total_entries", numericIndexStatus.get("total_entries") + stringIndexStatus.get("total_entries"),
                            "total_used_bytes", numericIndexStatus.get("total_used_bytes") + stringIndexStatus.get("total_used_bytes"),
                            "load_time", Math.max(numericIndexStatus.get("load_time"), stringIndexStatus.get("load_time")));
                } else {
                    if (numericIndexStatus.get("percent_complete") != 100L) {
                        return (I) Map.of("percent_complete", numericIndexStatus.get("percent_complete"),
                                "total_entries", stringIndexStatus.get("total_entries") + numericIndexStatus.get("total_entries"),
                                "total_used_bytes", stringIndexStatus.get("total_used_bytes") + numericIndexStatus.get("total_used_bytes"),
                                "load_time", Math.max(numericIndexStatus.get("load_time"), stringIndexStatus.get("load_time")));
                    } else {
                        return (I) Map.of("percent_complete", stringIndexStatus.get("percent_complete"),
                                "total_entries", stringIndexStatus.get("total_entries") + numericIndexStatus.get("total_entries"),
                                "total_used_bytes", stringIndexStatus.get("total_used_bytes") + numericIndexStatus.get("total_used_bytes"),
                                "load_time", Math.max(numericIndexStatus.get("load_time"), stringIndexStatus.get("load_time")));
                    }
                }
            } catch (final IllegalStateException e) {
                throw new IllegalStateException("No index found for vertex property key '" + key + "'.");
            }
        }
    }
}
