package com.aerospike.firefly.bulkloader.exceptions;

import java.util.Set;

public class VertexHeaderNotFoundException extends HeaderNotFoundException {
    public VertexHeaderNotFoundException(final Set<String> notFound) {
        super(notFound);
    }
}
