package com.aerospike.firefly.bulkloader.structure;

import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static scala.collection.JavaConverters.seqAsJavaList;

public class SparkFireflyVertex extends SparkFireflyElement {
    private static final String DEFAULT_LABEL = "vertex";

    protected SparkFireflyVertex(final long id, final String label,
                                 final List<Map.Entry<String, Object>> properties) {
        super(id, label, properties);
    }

    public static SparkFireflyVertex createVertex(final GenericRowWithSchema row, final boolean ignoreParseFailedProperties) {
        final String[] headers = row.schema().fieldNames();
        String id = null;
        String label = null;
        final List<Map.Entry<String, Object>> properties = new ArrayList<>();
        for (final String header : headers) {
            if (header.equalsIgnoreCase(ID_HEADER)) {
                id = row.getAs(header);
                continue;
            }
            if (header.equalsIgnoreCase(LABEL_HEADER)) {
                label = row.getAs(header);
                continue;
            }
            if (row.getAs(header) == null) {
                continue;
            }
            try {
                final Map.Entry<String, Object> property = generateProperty(header, row.getAs(header));
                properties.add(property);
            } catch (final RuntimeException e) {
                if (!ignoreParseFailedProperties) {
                    throw e;
                }
            }
        }
        if (id == null) {
            throw new RuntimeException("Could not generate Vertex due to a required value being blank.");
        }
        if (label == null) {
            label = DEFAULT_LABEL;
        }
        return new SparkFireflyVertex(Long.parseLong(id), label, properties);
    }

    @Override
    public FireflyId getId() {
        return FireflyId.of(FireflyVertex.class, this.id);
    }
}
