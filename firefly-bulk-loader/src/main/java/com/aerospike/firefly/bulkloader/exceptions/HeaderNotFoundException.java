package com.aerospike.firefly.bulkloader.exceptions;

import java.io.File;
import java.util.Set;

public class HeaderNotFoundException extends RuntimeException {
    public HeaderNotFoundException(final Set<String> notFound, final File file) {
        super("Required headers were not found in file " + file.getName() + ": " + String.join(", ", notFound));
    }
}

