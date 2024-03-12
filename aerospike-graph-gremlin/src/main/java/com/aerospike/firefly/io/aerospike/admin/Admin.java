package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.client.Info;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.sindex.SindexServiceBase;
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
    public static final Index index = new Index();

    public static class Index<I> {
        public <A> I getIndexList(final FireflyGraph firefly, final AdminContext<A> adminContext) {
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
                    final Map<String, Long> indexInfo = (Map<String, Long>) getStatusVertexPropertyIndex(firefly, index, adminContext);
                    if (indexInfo.get("percent_complete") == 100L) {
                        validVertexPropertyIndexes.add(index);
                    }
                } catch (final IllegalStateException ignored) {
                    // Do nothing.
                }
            }

            final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();
            try {
                if (vertexLabelIndex && getIndexStatus(firefly, firefly.getBaseGraph().V_LABEL_INDEX_NAME, adminContext).get("percent_complete") == 100L) {
                    validVertexPropertyIndexes.add("vertex.~label");
                }
            } catch (final IllegalStateException ignored) {
                // Do nothing.
            }

            // Return as a list so it can be used programatically.
            return (I) validVertexPropertyIndexes;
        }

        public <A> I getIndexCardinality(final FireflyGraph firefly, final AdminContext<A> adminContext) {
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

        public <A> I createVertexPropertyIndex(final FireflyGraph firefly, final String key, final AdminContext<A> adminContext) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            final List<String> existingIndexes = getExistingIndexes(firefly);
            String formattedIndex = String.format("%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key);
            firefly.getBaseGraph().createIndexBackground(existingIndexes,
                    set,
                    formattedIndex + "_" + STRING,
                    firefly.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                    STRING,
                    IndexCollectionType.DEFAULT,
                    true,
                    CTX.mapKey(Value.get(key)));
            formattedIndex = String.format("%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key);
            firefly.getBaseGraph().createIndexBackground(existingIndexes,
                    set,
                    formattedIndex + "_" + NUMERIC,
                    firefly.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                    NUMERIC,
                    IndexCollectionType.DEFAULT,
                    true,
                    CTX.mapKey(Value.get(key)));
            return (I) ("Vertex index creation of property key '" + key + "' in progress.");
        }

        public <A> I dropVertexPropertyIndex(final FireflyGraph firefly, final String key, final AdminContext<A> adminContext) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set,
                    String.format("%s_%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key, STRING));
            firefly.getBaseGraph().dropIndexBackground(set,
                    String.format("%s_%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key, NUMERIC));
            return (I) ("Vertex index of property key '" + key + "' dropped.");
        }

        private static List<String> getExistingIndexes(final FireflyGraph firefly) {
            return AerospikeConnection.InfoOps.
                    listExistingIndexes(firefly.getBaseGraph().getClient(), firefly.getBaseGraph().getNamespace()).
                    stream().map(Map.Entry::getKey).collect(Collectors.toList());
        }

        public <A> I createVertexLabelIndex(final FireflyGraph firefly, final AdminContext<A> adminContext) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            final List<String> existingIndexes = getExistingIndexes(firefly);
            firefly.getBaseGraph().createIndexBackground(existingIndexes,
                    set,
                    firefly.getBaseGraph().V_LABEL_INDEX_NAME,
                    firefly.getBaseGraph().LABEL_BIN,
                    IndexType.STRING,
                    IndexCollectionType.DEFAULT,
                    true);
            return (I) "Vertex label index creation in progress.";
        }

        public <A> I dropVertexLabelIndex(final FireflyGraph firefly, final AdminContext<A> adminContext) {
            final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
            firefly.getBaseGraph().dropIndexBackground(set, firefly.getBaseGraph().V_LABEL_INDEX_NAME);
            return (I) "Vertex label index dropped.";
        }

        public <A> I getStatusVertexLabelIndex(final FireflyGraph firefly, final AdminContext<A> adminContext) {
            try {
                return (I) getIndexStatus(firefly, firefly.getBaseGraph().V_LABEL_INDEX_NAME, adminContext);
            } catch (final IllegalStateException e) {
                throw new IllegalStateException("No index found on vertex label.");
            }
        }

        public static <A> Map<String, Long> getIndexStatus(final FireflyGraph firefly, final String indexName, final AdminContext<A> adminContext) {
            final String infoQueryFormat = "sindex/%s/%s"; // "sindex/<namespace>/<index name>
            int lowestLoadPct = 100;
            int totalEntries = 0;
            int totalUsedBytes = 0;
            int highestLoadTime = 0;
            boolean valid = false;
            for (final Node node : firefly.getBaseGraph().getClient().getNodes()) {
                final String infoResponse = Info.request(new InfoPolicy(), node,
                        String.format(infoQueryFormat, firefly.getBaseGraph().getNamespace(), indexName));
                for (String s : infoResponse.split(";")) {
                    valid = true;
                    if (s.startsWith("load_pct=")) {
                        final int loadPct = Integer.parseInt(s.split("=")[1]);
                        lowestLoadPct = Math.min(loadPct, lowestLoadPct);
                    } else if (s.startsWith("entries=")) {
                        final int entries = Integer.parseInt(s.split("=")[1]);
                        totalEntries += entries;
                    } else if (s.startsWith("used_bytes=")) {
                        final int usedBytes = Integer.parseInt(s.split("=")[1]);
                        totalUsedBytes += usedBytes;
                    } else if (s.startsWith("load_time=")) {
                        final int loadTime = Integer.parseInt(s.split("=")[1]);
                        highestLoadTime = Math.max(loadTime, highestLoadTime);
                    }
                }
            }
            if (valid) {
                // This is the case if the index is dropped.
                if (totalUsedBytes != 0) {
                    return Map.of("percent_complete", (long) lowestLoadPct,
                            "total_entries", (long) totalEntries,
                            "total_used_bytes", (long) totalUsedBytes,
                            "load_time", (long) highestLoadTime);
                }
            }
            throw new IllegalStateException("Index not found: " + indexName + ".");
        }

        public <A> I getStatusVertexPropertyIndex(final FireflyGraph firefly, final String key, final AdminContext<A> adminContext) {
            final String formattedIndex = String.format("%s_%s", firefly.getBaseGraph().getVpIndexPrefix(), key);
            final String stringIndexName = formattedIndex + "_" + STRING;
            final String numericIndexName = formattedIndex + "_" + NUMERIC;
            try {
                final Map<String, Long> numericIndexStatus = getIndexStatus(firefly, numericIndexName, adminContext);
                final Map<String, Long> stringIndexStatus = getIndexStatus(firefly, stringIndexName, adminContext);
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
