package com.aerospike.firefly.bulkloader.spark.structure;

import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SparkFireflyElement implements Serializable {
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
        // TODO: Cardinality support?
        final PropertyValueParser parser = new PropertyValueParser(nullValue);
        final int typeSpecifierIndex = header.lastIndexOf(":");
        if (typeSpecifierIndex == -1) {
            if (header.endsWith("(list)")) {
                final String propertyName = header.substring(0, header.length() - "(list)".length());
                if (value.isEmpty()) {
                    return new AbstractMap.SimpleEntry<>(propertyName, Collections.emptyList());
                } else {
                    final String[] values = value.split(";");
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
                if (!type.endsWith("(list)")) {
                    throw new IllegalArgumentException(
                            String.format("Invalid type '%s' for property '%s'. " +
                                    "Type should not contain parentheses unless it ends with '(list)'.", type, header));
                }
            }
            // Denote the list type and trim it off.
            if (type.endsWith("(list)")) {
                isList = true;
                type = type.substring(0, type.length() - "(list)".length());
            }
            if (type.endsWith("[]") && !type.startsWith("byte")) {
                throw new IllegalArgumentException(
                        String.format("Invalid type '%s' for property '%s'. " +
                                "Type should not end with '[]' unless it is a byte array 'byte[]'.", type, header));
            }
            final Object propertyValue;
            switch (type.toLowerCase()) {
                case "long":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseLong).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseLong(value);
                    }
                    break;
                case "int":
                case "integer":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseInt).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseInt(value);
                    }
                    break;
                case "double":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseDouble).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseDouble(value);
                    }
                    break;
                case "bool":
                case "boolean":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseBoolean).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseBoolean(value);
                    }
                    break;
                case "string":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseString).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseString(value);
                    }
                    break;
                case "date":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseDate).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseDate(value);
                    }
                    break;
                case "offsetdatetime":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseOffsetDateTime).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseOffsetDateTime(value);
                    }
                    break;
                case "byte":
                    if (isList) {
                        final String[] values = value.split(";");
                        propertyValue = Arrays.stream(values).map(parser::parseByte).collect(Collectors.toList());
                    } else {
                        propertyValue = parser.parseByte(value);
                    }
                    break;
                case "byte[]":
                    if (isList) {
                        propertyValue = new ArrayList<byte[]>();
                        final String[] values = value.split(";");
                        for (final String val : values) {
                            final String[] parts = val.split(",");
                            final byte[] bytes = new byte[parts.length];
                            for (int i = 0; i < parts.length; i++) {
                                bytes[i] = (byte) parser.parseByte(parts[i]);
                            }
                            ((List<byte[]>) propertyValue).add(bytes);
                        }
                    } else {
                        final String[] values = value.split(",");
                        final byte[] bytes = new byte[values.length];
                        for (int i = 0; i < values.length; i++) {
                            bytes[i] = (byte) parser.parseByte(values[i]);
                        }
                        propertyValue = bytes;
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
