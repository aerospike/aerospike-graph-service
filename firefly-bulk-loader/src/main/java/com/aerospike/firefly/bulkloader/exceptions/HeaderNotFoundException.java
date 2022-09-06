package com.aerospike.firefly.bulkloader.exceptions;

import java.util.Set;

public class HeaderNotFoundException extends RuntimeException {
    public HeaderNotFoundException(Set<String> notFound) {
        super("Required headers were not found: " + String.join(",", notFound));
    }
}

