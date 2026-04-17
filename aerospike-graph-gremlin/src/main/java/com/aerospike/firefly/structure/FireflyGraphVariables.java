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

package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Optional;
import java.util.Set;

public class FireflyGraphVariables implements Graph.Variables {
    private final FireflyGraph graph;

    public FireflyGraphVariables(FireflyGraph graph) {
        this.graph = graph;
    }

    @Override
    public Set<String> keys() {
        return graph.readGraphVariableKeys();
    }

    @Override
    public <R> Optional<R> get(String key) {
        return Optional.ofNullable(graph.readGraphVariable(key));
    }

    @Override
    public void set(String key, Object value) {
        if (null == value)
            throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        if (null == key || key.isEmpty())
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        graph.writeGraphVariable(key, FireflyHelper.validateGraphVariableValue(value));
    }

    @Override
    public void remove(String key) {
        graph.removeGraphVariable(key);
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
