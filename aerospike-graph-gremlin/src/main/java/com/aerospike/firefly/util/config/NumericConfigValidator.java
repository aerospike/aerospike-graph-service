package com.aerospike.firefly.util.config;

import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class NumericConfigValidator<T extends Number> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(NumericConfigValidator.class);

    private final Number min;
    private final Number max;
    protected final Map<String, Number> minimums = new HashMap<>();
    protected final Map<String, Number> maximums = new HashMap<>();

    protected NumericConfigValidator(final Number min, final Number max) {
        this.min = min;
        this.max = max;
    }

    public void addConfig(final String key, final Number min, final Number max) {
        this.minimums.put(key, min);
        this.maximums.put(key, max);
    }

    public void addConfigMin(final String key, final Number min) {
        this.minimums.put(key, min);
        this.maximums.put(key, this.max);
    }

    public void addConfigMax(final String key, final Number max) {
        this.maximums.put(key, max);
        this.minimums.put(key, this.min);
    }

    public T validate(final String key, final String value) {
        try {
            final Number parsedValue = parseValue(value.trim());
            if (minimums.containsKey(key) && underMinimum(parsedValue, key)) {
                final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is below the minimum acceptable value, \"" + minimums.get(key) + "\".";
                LOG.error(errorMessage);
                throw new ConfigurationRuntimeException(errorMessage);
            } else if (maximums.containsKey(key) && overMaximum(parsedValue, key)) {
                final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is above the maximum acceptable value, \"" + maximums.get(key) + "\".";
                LOG.error(errorMessage);
                throw new ConfigurationRuntimeException(errorMessage);
            }
            return getValue(parsedValue);
        } catch (final NumberFormatException e) {
            final String errorMessage = "Value provided, \"" + value + "\", for configuration key, \"" + key + "\", is invalid due to not being numeric.";
            LOG.error(errorMessage);
            throw new ConfigurationRuntimeException(errorMessage);
        }
    }

    public Set<String> keySet() {
        return Stream.concat(minimums.keySet().stream(), maximums.keySet().stream())
                .collect(Collectors.toSet());
    }

    protected abstract Number parseValue(final String value);

    protected abstract T getValue(final Number number);

    protected abstract boolean underMinimum(final Number parsedValue, final String key);

    protected abstract boolean overMaximum(final Number parsedValue, final String key);
}
