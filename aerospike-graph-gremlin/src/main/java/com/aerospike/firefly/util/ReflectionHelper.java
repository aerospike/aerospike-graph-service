package com.aerospike.firefly.util;

import java.lang.reflect.Field;

public class ReflectionHelper {
    private ReflectionHelper() {
    }

    public static Object getFieldValue(final Class clazz, final Object object, final String fieldName) {
        try {
            final Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            // This should never happen.
            throw new RuntimeException("Unexpected exception accessing " + fieldName + ". Please contact support.");
        }
    }

    public static Object getFieldValue(final Object object, final String fieldName) {
        return getFieldValue(object.getClass(), object, fieldName);
    }

    public static Object getSuperFieldValue(final Object object, final String fieldName) {
        return getFieldValue(object.getClass().getSuperclass(), object, fieldName);
    }

    public static void setFieldValue(final Class clazz, final Object object, final String fieldName, final Object value) {
        try {
            final Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            // This should never happen.
            throw new RuntimeException("Unexpected exception writing " + fieldName + ". Please contact support.");
        }
    }

    public static void setFieldValue(final Object object, final String fieldName, final Object value) {
        setFieldValue(object.getClass(), object, fieldName, value);
    }
}
