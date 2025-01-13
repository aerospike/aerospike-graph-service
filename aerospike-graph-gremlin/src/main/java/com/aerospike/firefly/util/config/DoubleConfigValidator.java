package com.aerospike.firefly.util.config;

public class DoubleConfigValidator extends NumericConfigValidator<Double> {

    public DoubleConfigValidator() {
        super(Double.MIN_VALUE, Double.MAX_VALUE);
    }

    @Override
    protected Number parseValue(final String value) {
        return Double.parseDouble(value);
    }

    @Override
    protected Double getValue(final Number number) {
        return number.doubleValue();
    }

    @Override
    protected boolean underMinimum(final Number parsedValue, final String key) {
        return parsedValue.doubleValue() < this.minimums.get(key).doubleValue();
    }

    @Override
    protected boolean overMaximum(final Number parsedValue, final String key) {
        return parsedValue.doubleValue() > this.maximums.get(key).doubleValue();
    }
}
