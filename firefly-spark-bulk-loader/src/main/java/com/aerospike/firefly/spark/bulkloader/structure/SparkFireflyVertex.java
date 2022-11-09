package com.aerospike.firefly.spark.bulkloader.structure;

import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkFireflyVertex extends SparkFireflyElement {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFireflyVertex.class);
    private static final String DEFAULT_LABEL = "vertex";

    private SparkFireflyVertex(final long id, final String label,
                                 final List<Map.Entry<String, Object>> properties) {
        super(id, label, properties);
    }

    public static SparkFireflyVertex createVertex(final GenericRowWithSchema row,
                                                  final boolean ignoreParseFailedProperties) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (final String header : headers) {
            if (row.getAs(header) == null) {
                continue;
            }
            if (header.equalsIgnoreCase(ID_HEADER)) {
                id = row.getAs(header);
                continue;
            }
            if (header.equalsIgnoreCase(LABEL_HEADER)) {
                label = row.getAs(header);
                continue;
            }
            try {
                final Map.Entry<String, Object> property = generateProperty(header, row.getAs(header));
                properties.add(property);
            } catch (final RuntimeException e) {
                LOG.warn("Failed to generate property for header '" + header + "' from value: " + row.getAs(header), e);
                if (!ignoreParseFailedProperties) {
                    throw e;
                }
            }
        }
        if (id == null) {
            throw new RuntimeException("Could not generate vertex due to a required value being blank.");
        }
        if (label == null) {
            label = DEFAULT_LABEL;
        }
        return new SparkFireflyVertex(Long.parseLong(id), label, properties);
    }

    @Override
    public FireflyId getFireflyId() {
        return FireflyIdFactory.createId(this.id);
    }
}
