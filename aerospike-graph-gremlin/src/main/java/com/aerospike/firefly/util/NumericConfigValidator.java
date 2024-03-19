package com.aerospike.firefly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class NumericConfigValidator implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(NumericConfigValidator.class);
    private final Map<String, Integer> minimums = new HashMap<>();
    private final Map<String, Integer> maximums = new HashMap<>();

    public void addConfig(final String key, final int min, final int max) {
        addConfigMin(key, min);
        addConfigMax(key, max);
    }

    public void addConfigMin(final String key, final int min) {
        this.minimums.put(key, min);
        this.maximums.put(key, Integer.MAX_VALUE);
    }

    public void addConfigMax(final String key, final int max) {
        this.maximums.put(key, max);
        this.minimums.put(key, Integer.MIN_VALUE);
    }

    public int validate(final String key, final String value) {
        try {
            final int valueInt = Integer.parseInt(value.trim());
            if (minimums.containsKey(key) && valueInt < minimums.get(key)) {
                final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is below the minimum acceptable value, \"" + minimums.get(key) + "\".";
                LOG.error(errorMessage);
                throw new RuntimeException(errorMessage);
            } else if (maximums.containsKey(key) && valueInt > maximums.get(key)) {
                final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is above the maximum acceptable value, \"" + maximums.get(key) + "\".";
                LOG.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
            return valueInt;
        } catch (final NumberFormatException e) {
            final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is invalid due to not being numeric.";
            LOG.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }
}
