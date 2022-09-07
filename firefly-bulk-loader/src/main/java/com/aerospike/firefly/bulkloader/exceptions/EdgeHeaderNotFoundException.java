package com.aerospike.firefly.bulkloader.exceptions;

import java.util.Set;

public class EdgeHeaderNotFoundException extends HeaderNotFoundException {
    public EdgeHeaderNotFoundException(final Set<String> notFound) {
        super(notFound);
    }
}
