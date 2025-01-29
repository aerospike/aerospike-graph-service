package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.codec.decoder.RowDecoder;
import com.aerospike.firefly.olap.codec.decoder.RowDecoderFactory;
import com.aerospike.firefly.olap.codec.encoder.RowEncoder;
import com.aerospike.firefly.olap.codec.encoder.RowEncoderFactory;
import com.aerospike.firefly.olap.codec.schema.RowSchema;
import com.aerospike.firefly.olap.codec.schema.RowSchemaFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class Codec {
    private final RowEncoder encoder;
    private final RowDecoder decoder;
    private final RowSchema schema;

    public Codec(final Set<TraverserRequirement> traverserRequirements) {
        this.encoder = RowEncoderFactory.getInstance().getEncoder(traverserRequirements);
        this.decoder = RowDecoderFactory.getInstance().getDecoder(traverserRequirements);
        this.schema = RowSchemaFactory.getInstance().getSchema(traverserRequirements);
    }

    public Row encode(final Traverser traverser) {
        return encoder.encode(traverser);
    }

    public Row encode(final Vertex vertex, final String step) {
        return encoder.encode(vertex, step);
    }

    public Row encode(final Edge edge, final String step) {
        return encoder.encode(edge, step);
    }

    public Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        return decoder.decode(row, tg, tm);
    }

    public StructType getSchema() {
        return schema.getSchema();
    }


    public static Object getId(final String id, final int idTypeOrdinal) {
        if (ID_TYPE.STRING.ordinal() == idTypeOrdinal) {
            return id;
        } else if (ID_TYPE.LONG.ordinal() == idTypeOrdinal) {
            return Long.parseLong(id);
        } else if (ID_TYPE.INTEGER.ordinal() == idTypeOrdinal) {
            return Integer.parseInt(id);
        } else {
            // TODO.
            throw new IllegalArgumentException("Only string int and long ids are supported in olap");
        }
    }

    public enum TRAVERSER_TYPE {
        VERTEX,
        EDGE
    }

    public enum ID_TYPE {
        STRING,
        INTEGER,
        LONG
    }

    public static ID_TYPE getIdType(final Object id) {
        if (id instanceof Long) {
            return Codec.ID_TYPE.LONG;
        } else if (id instanceof Integer) {
            return Codec.ID_TYPE.INTEGER;
        } else if (id instanceof  String) {
            return Codec.ID_TYPE.STRING;
        } else {
            // TODO.
            throw new IllegalArgumentException("Only Long string and integer types can be serialized at this time.");
        }
    }
}
