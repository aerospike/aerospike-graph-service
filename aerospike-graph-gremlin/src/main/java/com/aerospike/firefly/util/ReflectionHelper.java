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

    public static void setFieldValue(final Class clazz, final Object object, final String fieldName, final Object value) {
        try {
            Field field = null;
            if (fieldName.equals("path")) {
                Class c = clazz;
                do {
                    try {
                        field = c.getDeclaredField(fieldName);
                        break;
                    } catch (final NoSuchFieldException ignored) {
                        // Ignore and search parent class
                        c = c.getSuperclass();
                    }
                } while (c != null);

                if (field == null) {
                    throw new NoSuchFieldException();
                }
            } else {
                field = clazz.getDeclaredField(fieldName);
            }
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
