package com.aerospike.firefly.bulkloader.io;

import com.aerospike.firefly.bulkloader.exceptions.EdgeHeaderNotFoundException;
import com.aerospike.firefly.bulkloader.exceptions.VertexHeaderNotFoundException;
import com.aerospike.firefly.bulkloader.structure.BulkLoaderElement;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ElementReader<T extends BulkLoaderElement> {
    static private Logger LOG = LoggerFactory.getLogger(ElementReader.class);
    static protected String PROVIDED_ID_HEADER = "~providedId";
    protected final List<File> files;
    protected boolean generateId;
    protected BidiMap<String, Long> idMap;
    protected final AtomicLong generatedId = new AtomicLong();
    protected List<T> elements;

    public ElementReader(File directory, boolean generateId) {
        List<File> validFiles = getValidFiles(directory);
        if (validFiles.isEmpty()) {
            LOG.warn("Input is not valid for loading or is not a directory containing valid files for loading: " + directory);
        }
        this.files = validFiles;
        this.generateId = generateId;
        this.idMap = new DualHashBidiMap<>();
    }

    public List<T> read() {
        if (this.elements == null) {
            this.elements = new ArrayList<>();
        } else {
            return this.elements;
        }

        try {
            for (File file : this.files) {
                try (FileReader fileReader = new FileReader(file, StandardCharsets.UTF_8)) {
                    var csvReader = new CSVReader(fileReader);
                    String[] headers = csvReader.readNext();
                    verifyHeaders(getRequiredHeaders(), headers, true);

                    String[] elementRow;
                    while ((elementRow = csvReader.readNext()) != null) {
                        T element = generateElement(headers, elementRow);
                        this.elements.add(element);
                    }
                } catch (CsvValidationException cve) {
                    LOG.error("Error reading csv file line: ", cve);
                    throw new RuntimeException(cve);
                }
            }
        } catch (IOException ioe) {
            LOG.error("Error reading csv file: ", ioe);
            throw new RuntimeException(ioe);
        }
        return this.elements;
    }

    public BidiMap<String, Long> getIdMap() {
        return this.idMap;
    }

    protected abstract T generateElement(String[] headers, String[] elementRow);

    protected abstract String[] getRequiredHeaders();

    static protected void verifyHeaders(String[] requiredHeaders, String[] headers, boolean isVertex) {
        HashSet<String> notFoundHeaders = new HashSet<>(Arrays.asList(requiredHeaders));
        if (headers == null) {
            if (isVertex) {
                throw new VertexHeaderNotFoundException(notFoundHeaders);
            } else {
                throw new EdgeHeaderNotFoundException(notFoundHeaders);
            }
        }
        for (String header : headers) {
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

    static protected Map.Entry<String, Object> generateProperty(String property, String value) {
        // TODO: Type and Cardinality specifiers - currently only supports text
        return new AbstractMap.SimpleEntry<>(property, value);
    }

    static private List<File> getValidFiles(File directory) {
        List<File> validFiles = new ArrayList<>();
        if (directory.isDirectory()) {
            for (File file : directory.listFiles()) {
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
}
