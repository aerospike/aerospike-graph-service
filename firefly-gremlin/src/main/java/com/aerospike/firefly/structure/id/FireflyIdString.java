package com.aerospike.firefly.structure.id;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdString extends FireflyId {
    private final String id;

    // Package private. Only the factory should be instantiating this.
    FireflyIdString(final String id) {
        this.id = id;
    }

    @Override
    public String getUserId() {
        return id;
    }

    @Override
    public String getStorageId() {
        return id;
    }

    @Override
    public Long getStorageTypeIdx() {
        return 5L;
    }

    @Override
    public String getCachedId() {
        return getStorageId();
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return id.equals(((FireflyIdString) o).id);
    }
}
