package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.structure.BulkLoaderEdge;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EdgeReader extends ElementReader<BulkLoaderEdge> {
    static private final String ID_HEADER = "~id";
    static private final String FROM_HEADER = "~from";
    static private final String TO_HEADER = "~to";
    static private final String LABEL_HEADER = "~label";
    static private final String DEFAULT_LABEL = "edge";
    static private final String[] REQUIRED_HEADERS = new String[]{ID_HEADER, FROM_HEADER, TO_HEADER};

    public EdgeReader(final File directory) {
        super(directory);
    }

    @Override
    protected BulkLoaderEdge generateElement(final String[] headers, final String[] elementRow) {
        String id = "";
        String from = "";
        String to = "";
        String label = DEFAULT_LABEL;
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (int i = 0; i < elementRow.length; i++) {
            if (headers[i].equals(ID_HEADER)) {
                id = elementRow[i];
                properties.add(generateProperty(PROVIDED_ID_HEADER, elementRow[i]));
                continue;
            }
            if (headers[i].equals(FROM_HEADER)) {
                from = elementRow[i];
                continue;
            }
            if (headers[i].equals(TO_HEADER)) {
                to = elementRow[i];
                continue;
            }
            if (headers[i].equals(LABEL_HEADER)) {
                label = elementRow[i];
                continue;
            }
            properties.add(generateProperty(headers[i], elementRow[i]));
        }
        if (id.isBlank() || from.isBlank() || to.isBlank()) {
            throw new RuntimeException("Could not generate Edge due to a required value being blank.");
        }
        return new BulkLoaderEdge(id, label, from, to, properties);
    }

    @Override
    protected String[] getRequiredHeaders() {
        return REQUIRED_HEADERS;
    }
}
