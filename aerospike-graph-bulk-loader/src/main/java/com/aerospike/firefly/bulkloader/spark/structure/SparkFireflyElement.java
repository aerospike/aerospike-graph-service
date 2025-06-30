package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.exception.InvalidCsvHeaderException;
import com.aerospike.firefly.structure.id.FireflyId;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SparkFireflyElement implements Serializable {
    private static final String TYPE_DELIMITER = ":";
    private static final String MULTI_DELIMITER = ";";
    private static final String LIST_CARDINALITY = "(list)";

    public static final String ID_HEADER = "~id";
    public static final String LABEL_HEADER = "~label";

    public final Object id;
    protected final String label;
    protected final List<Map.Entry<String, Object>> properties;

    protected SparkFireflyElement(final Object id, final String label,
                                  final List<Map.Entry<String, Object>> properties) {
        this.id = id;
        this.label = label;
        this.properties = properties;
    }

    public abstract FireflyId getFireflyId(final AerospikeConnection db);

    public Object getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public List<Map.Entry<String, Object>> getProperties() {
        return this.properties;
    }

    protected static Map.Entry<String, Object> generateProperty(final String header,
                                                                final String value,
                                                                final String nullValue) {
        final PropertyValueParser parser = new PropertyValueParser(nullValue);
        final int typeSpecifierIndex = header.lastIndexOf(TYPE_DELIMITER);
        if (typeSpecifierIndex == -1) {
            if (header.endsWith(LIST_CARDINALITY)) {
                final String propertyName = header.substring(0, header.length() - LIST_CARDINALITY.length());
                if (value.isEmpty()) {
                    return new AbstractMap.SimpleEntry<>(propertyName, Collections.emptyList());
                } else {
                    final String[] values = value.split(MULTI_DELIMITER);
                    return new AbstractMap.SimpleEntry<>(propertyName,
                            Arrays.stream(values).map(parser::parseString).collect(Collectors.toList()));
                }
            }
            return new AbstractMap.SimpleEntry<>(header, parser.parseString(value));
        } else {
            String propertyName = header.substring(0, typeSpecifierIndex);
            String type = header.substring(typeSpecifierIndex + 1);
            boolean isList = false;
            if (type.contains("(") || type.contains(")")) {
                if (!type.endsWith(LIST_CARDINALITY)) {
                    throw new InvalidCsvHeaderException(
                            String.format("Invalid type '%s' for property '%s'. " +
                                    "Type should not contain parentheses unless it ends with '%s'.",
                                    type, header, LIST_CARDINALITY));
                }
            }
            // Denote the list type and trim it off.
            if (type.endsWith(LIST_CARDINALITY)) {
                isList = true;
                type = type.substring(0, type.length() - LIST_CARDINALITY.length());
            }
            final Object propertyValue;
            switch (type.toLowerCase()) {
                case "long":
                    if (isList) {
                        final String[] values = value.split(MULTI_DELIMITER);
                        propertyValue = Arrays.stream(values).map(parser::parseLong).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseLong(value);
                    }
                    break;
                case "int":
                case "integer":
                    if (isList) {
                        final String[] values = value.split(MULTI_DELIMITER);
                        propertyValue = Arrays.stream(values).map(parser::parseInt).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseInt(value);
                    }
                    break;
                case "double":
                    if (isList) {
                        final String[] values = value.split(MULTI_DELIMITER);
                        propertyValue = Arrays.stream(values).map(parser::parseDouble).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseDouble(value);
                    }
                    break;
                case "bool":
                case "boolean":
                    if (isList) {
                        final String[] values = value.split(MULTI_DELIMITER);
                        propertyValue = Arrays.stream(values).map(parser::parseBoolean).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseBoolean(value);
                    }
                    break;
                case "string":
                    if (isList) {
                        final String[] values = value.split(MULTI_DELIMITER);
                        propertyValue = Arrays.stream(values).map(parser::parseString).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseString(value);
                    }
                    break;
                case "blob":
                    if (isList) {
                        final String[] values = value.split(MULTI_DELIMITER);
                        propertyValue = Arrays.stream(values).map(parser::parseBlob).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseBlob(value);
                    }
                    break;
                default:
                    propertyName = header;
                    propertyValue = parser.parseString(value);
            }
            return new AbstractMap.SimpleEntry<>(propertyName, propertyValue);
        }
    }
}
