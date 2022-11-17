package com.aerospike.firefly.structure.id;

import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIdNumeric extends FireflyId {
    private final Long id;

    // Can be null.
    private final Class userClass;

    private static Map<Class, GetUserId> CONVERT_TO_USER_CLASS = Map.of(
            Long.class, new GetLongId(),
            Integer.class, new GetIntegerId(),
            Double.class, new GetDoubleId(),
            String.class, new GetStringId()
    );

    private static Map<Class, Long> CONVERT_TO_STORAGE_IDX = Map.of(
            Long.class, 1L,
            Integer.class, 2L,
            Double.class, 3L,
            String.class, 5L
    );

    /**
     * Constructor for Numeric Firefly Id. Long class assumed.
     *
     * @param id        Id to construct with.
     */
    // Package private. Only the factory should be instantiating this.
    FireflyIdNumeric(final Number id) {
        this.id = id.longValue();
        this.userClass = Long.class;
    }

    /**
     * Constructor for Numeric Firefly Id.
     *
     * @param id        Id to construct with.
     * @param userClass Class to construct with.
     */
    // Package private. Only the factory should be instantiating this.
    FireflyIdNumeric(final Number id, final Class userClass) {
        this.id = id.longValue();
        this.userClass = userClass == null ? Long.class : userClass;

        if (!CONVERT_TO_STORAGE_IDX.containsKey(this.userClass)) {
            // Should not happen in production, but add case for it anyway.
            throw new RuntimeException(String.format("Error, cannot create numeric id with user class of %s.", this.userClass.getName()));
        }
    }

    @Override
    public Object getUserId() {
        if (CONVERT_TO_USER_CLASS.containsKey(userClass)) {
            return CONVERT_TO_USER_CLASS.get(userClass).getUserId(id);
        } else {
            throw new RuntimeException(String.format("Error, cannot convert numeric id to user class of %s.", userClass.getName()));
        }
    }

    @Override
    public Long getStorageId() {
        return id;
    }

    @Override
    public Long getStorageTypeIdx() {
        return CONVERT_TO_STORAGE_IDX.get(userClass);
    }

    @Override
    public Long getCachedId() {
        return getStorageId();
    }

    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof FireflyId) return getStorageId().equals(((FireflyId) o).getStorageId());
        return false;
    }

    static abstract class GetUserId {
        public abstract Object getUserId(final Number id);
    }

    static class GetLongId extends GetUserId {
        @Override
        public Object getUserId(final Number id) {
            return id.longValue();
        }
    }

    static class GetIntegerId extends GetUserId {
        @Override
        public Object getUserId(final Number id) {
            return id.intValue();
        }
    }

    static class GetDoubleId extends GetUserId {
        @Override
        public Object getUserId(final Number id) {
            return id.doubleValue();
        }
    }

    static class GetStringId extends GetUserId {
        @Override
        public Object getUserId(final Number id) {
            return id.toString();
        }
    }
}
