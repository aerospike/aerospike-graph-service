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

    public VertexReader(final File directory) {
        super(directory);
    }

    protected BulkLoaderVertex generateElement(final String[] headers, final String[] elementRow) {
        String id = "";
        String label = DEFAULT_LABEL;
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < elementRow.length; i++) {
            if (headers[i].equals(ID_HEADER)) {
                id = elementRow[i];
                properties.add(generateProperty(PROVIDED_ID_HEADER, elementRow[i]));
                continue;
            }
            if (headers[i].equals(LABEL_HEADER)) {
                label = elementRow[i];
                continue;
            }
            properties.add(generateProperty(headers[i], elementRow[i]));
        }
        if (id.isBlank()) {
            throw new RuntimeException("Could not generate Vertex due to a required value being blank.");
        }
        return new BulkLoaderVertex(id, label, properties);
    }

    protected String[] getRequiredHeaders() {
        return REQUIRED_HEADERS;
    }
}
