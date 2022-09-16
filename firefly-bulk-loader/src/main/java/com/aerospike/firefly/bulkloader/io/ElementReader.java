package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.exceptions.HeaderNotFoundException;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class ElementReader<T extends BulkLoaderElement> implements AutoCloseable {
    static protected final String PROVIDED_ID_HEADER = "~providedId";
    static private final Logger LOG = LoggerFactory.getLogger(ElementReader.class);
    protected final List<File> files;
    protected CSVReader currentReader = null;
    protected String[] currentHeaders = null;
    protected Map<String, PropertyTypeData> currentHeadersTypes = null;
    protected boolean isClosed = false;

    public ElementReader(final File directory) {
        final List<File> validFiles = validateAndGetFiles(directory);
        if (validFiles.isEmpty()) {
            LOG.warn("Input is not valid for loading or is not a directory containing valid files for loading: " +
                    directory);
        }
        this.files = validFiles;
        // TODO: Potentially multi-thread and buffer this
        loadNextFile();
    }

    public void close() {
        if (this.isClosed) {
            return;
        }
        this.isClosed = true;
        closeCurrentResources();
    }

    public T next() {
        if (this.isClosed) {
            return null;
        }
        try {
            final String[] elementRow = this.currentReader.readNext();
            if (elementRow == null) {
                loadNextFile();
                return next();
            }
            return generateElement(this.currentHeaders, elementRow);
        } catch (IOException | CsvValidationException e) {
            LOG.error("Error reading csv file: ", e);
            this.close();
            throw new RuntimeException(e);
        }
    }

    static protected void verifyHeaders(final String[] requiredHeaders, final String[] headers, final File file) {
        final HashSet<String> notFoundHeaders = new HashSet<>(Arrays.asList(requiredHeaders));
        if (headers == null) {
            throw new HeaderNotFoundException(notFoundHeaders, file);
        }
        for (final String header : headers) {
            notFoundHeaders.remove(header);
            if (notFoundHeaders.isEmpty()) {
                return;
            }
        }
        throw new HeaderNotFoundException(notFoundHeaders, file);
    }

    protected Map.Entry<String, Object> generateProperty(final String property, final String value) {
        final PropertyTypeData typeData = this.currentHeadersTypes.get(property);
        final boolean isList = typeData.isList;
        final Object typedValue;
        switch (typeData.type) {
            case LONG:
                typedValue = parseValue(isList, value, Long::parseLong);
                break;
            case INTEGER:
                typedValue = parseValue(isList, value, Integer::parseInt);
                break;
            case DOUBLE:
                typedValue = parseValue(isList, value, Double::parseDouble);
                break;
            case BOOLEAN:
                typedValue = parseValue(isList, value, Boolean::parseBoolean);
                break;
            case STRING:
                typedValue = parseValue(isList, value, v -> v);
                break;
            default:
                // This should never happen since unknown types default to STRING.
                throw new RuntimeException("Received unexpected type specifier that was not defaulted to text.");
        }
        return new AbstractMap.SimpleEntry<>(property, typedValue);
    }

    protected abstract T generateElement(String[] headers, String[] elementRow);

    protected abstract String[] getRequiredHeaders();

    static private Object parseValue(final boolean isList, final String value,
                                     final Function<String, Object> parseFunction) {
        if (isList) {
            final String[] values = value.split(";");
            return Arrays.stream(values).map(parseFunction).collect(Collectors.toList());
        } else {
            return parseFunction.apply(value);
        }
    }

    private void loadNextFile() {
        closeCurrentResources();
        if (this.files.isEmpty()) {
            this.isClosed = true;
            return;
        }
        final File nextFile = this.files.remove(0);
        try {
            final FileReader fileReader = new FileReader(nextFile, StandardCharsets.UTF_8);
            this.currentReader = new CSVReader(fileReader);
            // This throws CsvValidationException but should never happen since all file headers were pre-validated
            this.currentHeaders = parsePropertyHeaders(this.currentReader.readNext(), nextFile);
        } catch (final IOException | CsvValidationException e) {
            LOG.error("Error loading next csv file: ", e);
            this.close();
            throw new RuntimeException(e);
        }
    }

    private String[] parsePropertyHeaders(final String[] headers, final File file) {
        final Map<String, PropertyTypeData> headersTypes = new HashMap<>();
        headersTypes.put(PROVIDED_ID_HEADER, new PropertyTypeData(PropertyType.STRING, false));
        for (int i = 0; i < headers.length; i++) {
            // TODO: Cardinality support?
            final String property = headers[i];
            final int typeSpecifierIndex = property.lastIndexOf(":");
            if (typeSpecifierIndex == -1) {
                headersTypes.put(property, new PropertyTypeData(PropertyType.STRING, false));
            } else {
                String propertyName = property.substring(0, typeSpecifierIndex);
                String type = property.substring(typeSpecifierIndex + 1);
                boolean isList = false;
                if (type.endsWith("[]")) {
                    isList = true;
                    type = type.substring(0, type.length() - 2);
                }
                final PropertyType propertyType;
                switch (type.toLowerCase()) {
                    case "long":
                        propertyType = PropertyType.LONG;
                        break;
                    case "int":
                    case "integer":
                        propertyType = PropertyType.INTEGER;
                        break;
                    case "double":
                        propertyType = PropertyType.DOUBLE;
                        break;
                    case "bool":
                    case "boolean":
                        propertyType = PropertyType.BOOLEAN;
                        break;
                    case "string":
                        propertyType = PropertyType.STRING;
                        break;
                    default:
                        LOG.warn("Type '" + type + "' for header '" + propertyName + "' in " + file.getName() + " is " +
                                "not a supported type. Falling back to '" + property +
                                "' as property name with values as text type.");
                        propertyName = property;
                        propertyType = PropertyType.STRING;
                }
                headersTypes.put(propertyName, new PropertyTypeData(propertyType, isList));
                headers[i] = propertyName;
            }
        }
        this.currentHeadersTypes = headersTypes;
        return headers;
    }

    private void closeCurrentResources() {
        try {
            if (this.currentReader != null) {
                this.currentReader.close();
            }
        } catch (final IOException e) {
            LOG.warn("Ignoring exception when closing resource: ", e);
        }
    }

    private List<File> validateAndGetFiles(final File directory) {
        final List<File> validFiles = new LinkedList<>();
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
        // Validating this here has overhead, but better to fail fast for a large amount of data.
        for (final File file : validFiles) {
            try (final FileReader fileReader = new FileReader(file, StandardCharsets.UTF_8);
                 final CSVReader csvReader = new CSVReader(fileReader)) {
                final String[] headers = csvReader.readNext();
                verifyHeaders(getRequiredHeaders(), headers, file);
            } catch (final CsvValidationException | IOException e) {
                LOG.error("Error reading " + file.getName() + " : ", e);
                throw new RuntimeException(e);
            }
        }
        return validFiles;
    }

    private enum PropertyType {
        LONG,
        INTEGER,
        DOUBLE,
        STRING,
        BOOLEAN
    }

    private class PropertyTypeData {
        private final PropertyType type;
        private final boolean isList;

        private PropertyTypeData(final PropertyType type, final boolean isList) {
            this.type = type;
            this.isList = isList;
        }
    }
}
