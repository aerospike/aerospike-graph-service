package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Serialization {
    public static <C> List<C> deserializeList(final List<byte[]> input, final Class<? extends C> clazz) {
        return input.stream().map(bytes -> deserializeObject(bytes, clazz)).collect(Collectors.toList());
    }

    public static byte[] serializeObject(final Object o) {
        final Io io = GryoIo.build(GryoVersion.V3_0).graph(EmptyGraph.instance()).onMapper(mapper -> {
            ((GryoMapper.Builder) mapper)
                    .create();
        }).create();
        final GraphWriter writer = io.writer().create();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            writer.writeObject(baos, o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    public static <C> C deserializeObject(final byte[] input, final Class<? extends C> clazz) {
        final Io io = GryoIo.build(GryoVersion.V3_0).graph(EmptyGraph.instance()).onMapper(mapper -> {
            ((GryoMapper.Builder) mapper)
                    .create();
        }).create();
        final GraphReader reader = io.reader().create();
        try {
            return reader.readObject(new ByteArrayInputStream(input), clazz); // readObject autodeduces type, but is slower do to autodeduction
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
