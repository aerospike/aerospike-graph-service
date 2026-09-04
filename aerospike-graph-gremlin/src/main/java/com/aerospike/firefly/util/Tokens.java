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

public final class Tokens {
    private Tokens() {
    }

    public static final String VERTEX_ID_COUNTER = "_vxidctr";
    public static final String EDGE_UNIQUE_ID_COUNTER = "_euidctr";
    public static final String EDGE_PACKING_ID_COUNTER = "_epidctr";
    public static final String VERTEX_PROPERTY_ID_COUNTER = "_vxpidctr";
    public static final String UNIMPLEMENTED = "unimplemented";

    public static final String VERTEX_LABEL_SCHEMA = "_vxlsch";
    public static final String VERTEX_PROPERTY_SCHEMA = "_vxpsch";
    public static final String GEO_PROPERTY_SCHEMA = "_geosch";
    public static final String VERTEX_PROPERTY_PROPERTY_SCHEMA = "_vppsch";
    public static final String EDGE_LABEL_SCHEMA = "_elsch";
    public static final String EDGE_PROPERTY_SCHEMA = "_epsch";
}
