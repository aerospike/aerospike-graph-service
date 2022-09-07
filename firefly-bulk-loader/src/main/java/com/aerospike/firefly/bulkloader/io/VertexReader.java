package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderVertex;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VertexReader extends ElementReader<BulkLoaderVertex> {
    static private final String ID_HEADER = "~id";
    static private final String LABEL_HEADER = "~label";
    static private final String DEFAULT_LABEL = "vertex";
    static private final String[] REQUIRED_HEADERS = new String[]{ID_HEADER};

    public VertexReader(final File directory, final boolean generateId) {
        super(directory, true, generateId);
    }

    protected BulkLoaderVertex generateElement(final String[] headers, final String[] elementRow) {
        long id = 0;
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
            if (headers[i].equals(LABEL_HEADER)) {
                label = elementRow[i];
                continue;
            }
            properties.add(generateProperty(headers[i], elementRow[i]));
        }
        return new BulkLoaderVertex(id, label, properties);
    }

    protected String[] getRequiredHeaders() {
        return REQUIRED_HEADERS;
    }
}
