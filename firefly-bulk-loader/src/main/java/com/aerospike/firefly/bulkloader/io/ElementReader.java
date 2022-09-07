package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.exceptions.EdgeHeaderNotFoundException;
import com.aerospike.firefly.bulkloader.exceptions.VertexHeaderNotFoundException;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderElement;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class ElementReader<T extends BulkLoaderElement> {
    static protected final String PROVIDED_ID_HEADER = "~providedId";
    static private final Logger LOG = LoggerFactory.getLogger(ElementReader.class);
    protected final List<File> files;
    protected final boolean isVertex;
    protected final boolean generateId;
    protected final AtomicLong generatedId;
    protected final Map<String, Long> idMap;
    protected List<T> elements;

    public ElementReader(final File directory, final boolean isVertex, final boolean generateId) {
        final List<File> validFiles = getValidFiles(directory);
        if (validFiles.isEmpty()) {
            LOG.warn("Input is not valid for loading or is not a directory containing valid files for loading: " +
                    directory);
        }
        this.files = validFiles;
        this.isVertex = isVertex;
        this.generateId = generateId;
        this.generatedId = new AtomicLong();
        this.idMap = new HashMap<>();
    }

    static protected void verifyHeaders(final String[] requiredHeaders, final String[] headers,
                                        final boolean isVertex) {
        final HashSet<String> notFoundHeaders = new HashSet<>(Arrays.asList(requiredHeaders));
        if (headers == null) {
            if (isVertex) {
                throw new VertexHeaderNotFoundException(notFoundHeaders);
            } else {
                throw new EdgeHeaderNotFoundException(notFoundHeaders);
            }
        }
        for (final String header : headers) {
            notFoundHeaders.remove(header);
            if (notFoundHeaders.isEmpty()) {
                return;
            }
        }
        if (isVertex) {
            throw new VertexHeaderNotFoundException(notFoundHeaders);
        } else {
            throw new EdgeHeaderNotFoundException(notFoundHeaders);
        }
    }

    static protected Map.Entry<String, Object> generateProperty(final String property, final String value) {
        // TODO: Cardinality support?
        final int typeSpecifierIndex = property.lastIndexOf(":");
        if (typeSpecifierIndex == -1) {
            return new AbstractMap.SimpleEntry<>(property, value);
        } else {
            String propertyName = property.substring(0, typeSpecifierIndex);
            String type = property.substring(typeSpecifierIndex + 1);
            boolean isList = false;
            if (type.endsWith("[]")) {
                isList = true;
                type = type.substring(0, type.length() - 2);
            }
            final Object typedValue;
            switch (type.toLowerCase()) {
                case "long":
                    typedValue = parseValue(isList, value, Long::parseLong);
                    break;
                case "int":
                case "integer":
                    typedValue = parseValue(isList, value, Integer::parseInt);
                    break;
                case "double":
                    typedValue = parseValue(isList, value, Double::parseDouble);
                    break;
                case "bool":
                case "boolean":
                    typedValue = parseValue(isList, value, Boolean::parseBoolean);
                    break;
                case "string":
                    typedValue = parseValue(isList, value, v -> v);
                    break;
                default:
                    LOG.warn("Type '" + type + "' for header '" + propertyName + "' is not a supported type. " +
                            "Falling back to '" + property + "' as property name with values as text type.");
                    propertyName = property;
                    typedValue = value;
            }
            return new AbstractMap.SimpleEntry<>(propertyName, typedValue);
        }
    }

    static private Object parseValue(final boolean isList, final String value,
                                     final Function<String, Object> parseFunction) {
        if (isList) {
            final String[] values = value.split(";");
            return Arrays.stream(values).map(parseFunction).collect(Collectors.toList());
        } else {
            return parseFunction.apply(value);
        }
    }

    static private List<File> getValidFiles(final File directory) {
        final List<File> validFiles = new ArrayList<>();
        if (directory.isDirectory()) {
            for (final File file : directory.listFiles()) {
                if (FilenameUtils.isExtension(file.getName(), "csv")) {
                    LOG.info("Found valid file for loading: " + file.getName());
                    validFiles.add(file);
                } else {
                    LOG.info("Ignoring invalid file for loading found in directory: " + file.getName());
                }
            }
        } else if (FilenameUtils.isExtension(directory.getName(), "csv")) {
            LOG.info("Found valid file for loading: " + directory.getName());
            validFiles.add(directory);
        }
        return validFiles;
    }

    public List<T> read() {
        if (this.elements == null) {
            this.elements = new ArrayList<>();
        } else {
            return this.elements;
        }

        try {
            for (final File file : this.files) {
                try (final FileReader fileReader = new FileReader(file, StandardCharsets.UTF_8)) {
                    final var csvReader = new CSVReader(fileReader);
                    final String[] headers = csvReader.readNext();
                    verifyHeaders(getRequiredHeaders(), headers, this.isVertex);

                    String[] elementRow;
                    while ((elementRow = csvReader.readNext()) != null) {
                        final T element = generateElement(headers, elementRow);
                        this.elements.add(element);
                    }
                } catch (final CsvValidationException cve) {
                    LOG.error("Error reading csv file line: ", cve);
                    throw new RuntimeException(cve);
                }
            }
        } catch (final IOException ioe) {
            LOG.error("Error reading csv file: ", ioe);
            throw new RuntimeException(ioe);
        }
        return this.elements;
    }

    public Map<String, Long> getIdMap() {
        return this.idMap;
    }

    protected abstract T generateElement(String[] headers, String[] elementRow);

    protected abstract String[] getRequiredHeaders();
}
