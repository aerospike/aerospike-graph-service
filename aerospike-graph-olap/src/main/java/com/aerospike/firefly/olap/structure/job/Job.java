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

package com.aerospike.firefly.olap.structure.job;

import com.aerospike.client.Bin;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class Job {
    public enum State {
        STARTED, FINISHED, ERROR, CANCELLED
    }

    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    private final String id;
    private final String query;
    private final String vertexProgram;
    private final Map config;
    private final Date started;
    private Date finished = null;
    private State state = State.STARTED;
    private int resultsCount = -1;
    private String error;
    private int iteration = -1;

    static {
        final TimeZone tz = TimeZone.getTimeZone("UTC");
        df.setTimeZone(tz);
    }

    public Job(final String id, final String vertexProgram, final String query, final Configuration config) {
        this.vertexProgram = vertexProgram;
        this.started = new Date();
        this.id = id;
        this.query = query;

        this.config = new HashMap<>();
        final Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            final String key = keys.next();
            final String value = config.getString(key);
            this.config.put(key, value.substring(0, Math.min(value.length(), 100)));
        }
    }

    public Job(final com.aerospike.client.Record record) {
        id = record.getString("id");
        vertexProgram = record.getString("vertexProgram");
        query = record.getString("query");
        started = new Date(record.getLong("started"));
        iteration = record.getInt("iteration");
        state = State.valueOf(record.getString("state"));
        config = record.getMap("config");

        if (state != State.STARTED) {
            finished = new Date(record.getLong("finished"));
        }
        if (state == State.FINISHED) {
            resultsCount = record.getInt("resultsCount");
        }
        if (state == State.ERROR) {
            error = record.getString("error");
        }
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public void finish(final int resultsCount) {
        this.resultsCount = resultsCount;
        finished = new Date();
        state = State.FINISHED;
    }

    public void error(final String error) {
        this.error = error;
        finished = new Date();
        state = State.ERROR;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public void cancel() {
        state = State.CANCELLED;
        finished = new Date();
    }

    public String toString() {
        return String.format("Job %s; vertex program: %s; query: %s; state: %s", id, vertexProgram, query, state);
    }

    public Vertex toVertex() {
        final List<VertexProperty> props = new ArrayList<>();
        props.add(new DetachedVertexProperty(1, "state", state.toString(), null));
        props.add(new DetachedVertexProperty(8, "vertexProgram", vertexProgram, null));
        props.add(new DetachedVertexProperty(9, "config", config, null));
        props.add(new DetachedVertexProperty(2, "query", query, null));
        props.add(new DetachedVertexProperty(3, "started", started, null));
        props.add(new DetachedVertexProperty(4, "iteration", iteration, null));
        if (state != State.STARTED) {
            props.add(new DetachedVertexProperty(5, "finished", finished, null));
        }
        if (state == State.FINISHED) {
            props.add(new DetachedVertexProperty(6, "resultsCount", resultsCount, null));
        }
        if (state == State.ERROR) {
            props.add(new DetachedVertexProperty(7, "error", error, null));
        }

        return new DetachedVertex(id, "job", props);
    }

    public Bin[] asBins() {
        final List<Bin> bins = new ArrayList<>(7);
        // id is not necessary, but convenient to have
        bins.add(new Bin("id", id));
        bins.add(new Bin("state", state.toString()));
        bins.add(new Bin("vertexProgram", vertexProgram));
        bins.add(new Bin("config", config));
        bins.add(new Bin("query", query));
        bins.add(new Bin("started", started.getTime()));
        bins.add(new Bin("iteration", iteration));

        if (state != State.STARTED) {
            bins.add(new Bin("finished", finished.getTime()));
        }
        if (state == State.FINISHED) {
            bins.add(new Bin("resultsCount", resultsCount));
        }
        if (state == State.ERROR) {
            bins.add(new Bin("error", error));
        }
        return bins.toArray(new Bin[0]);
    }
}
