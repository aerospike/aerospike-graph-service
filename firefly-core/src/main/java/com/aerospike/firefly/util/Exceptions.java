package com.aerospike.firefly.util;

import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Exceptions {
    public static class Unimplemented extends RuntimeException {
        public Unimplemented() {
            super(UNIMPLEMENTED);
        }
    }
}

