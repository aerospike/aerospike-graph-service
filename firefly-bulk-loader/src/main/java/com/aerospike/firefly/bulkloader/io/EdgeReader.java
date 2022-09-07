package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EdgeReader extends ElementReader<BulkLoaderEdge> {
    static private final Logger LOG = LoggerFactory.getLogger(EdgeReader.class);
    static private final String ID_HEADER = "~id";
    static private final String FROM_HEADER = "~from";
    static private final String TO_HEADER = "~to";
    static private final String LABEL_HEADER = "~label";
    static private final String DEFAULT_LABEL = "edge";
    static private final String[] REQUIRED_HEADERS = new String[]{ID_HEADER, FROM_HEADER, TO_HEADER};
    private final Map<Long, FireflyVertex> vertexes;
    private final Map<String, Long> vertexIdMap;

    public EdgeReader(final File directory, final boolean generateId, final List<FireflyVertex> vertexes,
                      final Map<String, Long> vertexIdMap) {
        super(directory, false, generateId);
        final Map<Long, FireflyVertex> vertexMap = new HashMap<>();
        for (final FireflyVertex vertex : vertexes) {
            final Long id = (long) vertex.id();
            vertexMap.put(id, vertex);
        }
        this.vertexes = vertexMap;
        this.vertexIdMap = vertexIdMap;
    }

    @Override
    protected BulkLoaderEdge generateElement(final String[] headers, final String[] elementRow) {
        long id = 0;
        FireflyVertex from = null;
        FireflyVertex to = null;
        String label = DEFAULT_LABEL;
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < elementRow.length; i++) {
            if (headers[i].equals(ID_HEADER)) {
                final String idKey = elementRow[i];
                if (generateId) {
                    id = generatedId.getAndIncrement();
                    properties.add(generateProperty(PROVIDED_ID_HEADER, idKey));
                } else {
                    id = Long.parseLong(elementRow[i]);
                }
                this.idMap.put(idKey, id);
                continue;
            }
            if (headers[i].equals(FROM_HEADER)) {
                final String providedFromId = elementRow[i];
                final Long fromId = this.vertexIdMap.get(providedFromId);
                if (fromId == null) {
                    LOG.warn("Could not find generated long id for inserted vertex in map using key: " +
                            providedFromId);
                }
                from = this.vertexes.get(fromId);
                if (fromId == null) {
                    LOG.warn("Could not find loaded vertex with id: " + fromId);
                }
                continue;
            }
            if (headers[i].equals(TO_HEADER)) {
                final String providedToId = elementRow[i];
                final Long toId = this.vertexIdMap.get(providedToId);
                to = this.vertexes.get(toId);
                continue;
            }
            if (headers[i].equals(LABEL_HEADER)) {
                label = elementRow[i];
                continue;
            }
            properties.add(generateProperty(headers[i], elementRow[i]));
        }
        return new BulkLoaderEdge(id, label, from, to, properties);
    }

    @Override
    protected String[] getRequiredHeaders() {
        return REQUIRED_HEADERS;
    }
}
