package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.commons.collections4.BidiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EdgeReader extends ElementReader<BulkLoaderEdge> {
    static private Logger LOG = LoggerFactory.getLogger(EdgeReader.class);
    static private String ID_HEADER = "~id";
    static private String FROM_HEADER = "~from";
    static private String TO_HEADER = "~to";
    static private String LABEL_HEADER = "~label";
    static private String DEFAULT_LABEL = "edge";

    private Map<Long, FireflyVertex> vertexes;
    private BidiMap<String, Long> vertexIdMap;
    static private String[] REQUIRED_HEADERS = new String[]{ ID_HEADER, FROM_HEADER, TO_HEADER };

    public EdgeReader(File directory, boolean generateId, List<FireflyVertex> vertexes, BidiMap<String, Long> vertexIdMap) {
        super(directory, generateId);
        Map<Long, FireflyVertex> vertexMap = new HashMap<>();
        for (FireflyVertex vertex : vertexes) {
            Long id = (long)vertex.id();
            vertexMap.put(id, vertex);
        }
        this.vertexes = vertexMap;
        this.vertexIdMap = vertexIdMap;
    }

    @Override
    protected BulkLoaderEdge generateElement(String[] headers, String[] elementRow) {
        long id = 0;
        FireflyVertex from = null;
        FireflyVertex to = null;
        String label = DEFAULT_LABEL;
        List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < elementRow.length; i++) {
            if (headers[i].equals(ID_HEADER)) {
                String idKey = elementRow[i];
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
                String providedFromId = elementRow[i];
                Long fromId = this.vertexIdMap.get(providedFromId);
                if (fromId == null) {
                    LOG.warn("Could not find generated long id for inserted vertex in map using key: " + providedFromId);
                }
                from = this.vertexes.get(fromId);
                if (fromId == null) {
                    LOG.warn("Could not find loaded vertex with id: " + fromId);
                }
                continue;
            }
            if (headers[i].equals(TO_HEADER)) {
                String providedToId = elementRow[i];
                Long toId = this.vertexIdMap.get(providedToId);
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
