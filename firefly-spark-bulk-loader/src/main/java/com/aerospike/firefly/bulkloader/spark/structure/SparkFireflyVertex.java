package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdPoly;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SparkFireflyVertex extends SparkFireflyElement {
    private static final Logger LOG = LoggerFactory.getLogger(SparkFireflyVertex.class);
    private static final String DEFAULT_LABEL = "vertex";

    private SparkFireflyVertex(final Object id, final String label,
                                 final List<Map.Entry<String, Object>> properties) {
        super(id, label, properties);
    }

    public static SparkFireflyVertex createVertex(final GenericRowWithSchema row,
                                                  final String nullValue) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        final List<Map.Entry<String, Object>> properties = Collections.synchronizedList(new ArrayList<>());
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
                final String value = row.getAs(header);
                final Map.Entry<String, Object> property = generateProperty(header, value, nullValue);
                properties.add(property);
            } catch (final RuntimeException e) {
                LOG.error("Failed to generate property for header '" + header + "' from value: " + row.getAs(header));
                throw new FireflyBulkLoaderException(e);
            }
        }
        if (id == null) {
            throw new FireflyBulkLoaderException("Could not generate vertex due to ~id being blank.");
        }
        if (label == null) {
            label = DEFAULT_LABEL;
        }
        return new SparkFireflyVertex(PropertyValueParser.parseId(id), label, properties);
    }

    @Override
    public FireflyId getFireflyId(final AerospikeConnection db) {
        return FireflyIdPoly.fromObject(this.id, db.VERTEX_AERO_SET);
    }
}
