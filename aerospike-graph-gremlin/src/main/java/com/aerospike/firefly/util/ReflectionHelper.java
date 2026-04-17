/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
