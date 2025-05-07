package com.aerospike.firefly.util.config;

public class IntegerConfigValidator extends NumericConfigValidator<Integer> {

    public IntegerConfigValidator() {
        super(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Override
    protected Number parseValue(final String value) {
        return Integer.parseInt(value);
    }

    @Override
    protected Integer getValue(final Number number) {
        return number.intValue();
    }

    @Override
    protected boolean underMinimum(final Number parsedValue, final String key) {
        return parsedValue.intValue() < this.minimums.get(key).intValue();
    }

    @Override
    protected boolean overMaximum(final Number parsedValue, final String key) {
        return parsedValue.intValue() > this.maximums.get(key).intValue();
    }
}
