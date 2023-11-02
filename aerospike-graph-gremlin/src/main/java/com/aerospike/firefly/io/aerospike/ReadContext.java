package com.aerospike.firefly.io.aerospike;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ReadContext {
    private final String setName;
    private final Optional<String> binName;
    private final Optional<String> keyName;

    public ReadContext(final String setName, final Optional<String> binName, final Optional<String> keyName) {
        this.setName = setName;
        this.binName = binName;
        this.keyName = keyName;
    }

    public static ReadContext create(final String set) {
        return new ReadContext(set, Optional.empty(), Optional.empty());
    }

    public static ReadContext create(final String set, final String bin) {
        return new ReadContext(set, Optional.of(bin), Optional.empty());
    }

    public static ReadContext create(final String set, final String bin, final String key) {
        return new ReadContext(set, Optional.ofNullable(bin), Optional.ofNullable(key));
    }


    public String getSetName() {
        return setName;
    }

    public Optional<String> getBinName() {
        return binName;
    }

    public Optional<String> getKeyName() {
        return keyName;
    }

}
