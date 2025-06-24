package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class DateTimeUtil {

    public static void testDateTimePropertiesCases(GraphTraversalSource g) {
        final Vertex v1 = g.addV("added").next();

        final Date initD = getDate(2024, Calendar.FEBRUARY, 3);
        final Date initDT = getDate(2025, Calendar.MARCH, 14, 4, 5, 0, 0);

        OffsetDateTime initODT = OffsetDateTime.of(2023, 1, 7, 4, 5,
                6, 7, ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS);
        final List<Object> initList = new ArrayList<>();
        initList.add(initD);
        initList.add(initDT);
        initList.add(initODT);

        // Create a vertex with datetime properties.
        final Vertex v2 = g.addV("init")
                .property("initD", initD)
                .property("initDT", initDT)
                .property("initODT", initODT)
                .property("initList", initList)
                .next();
        // Create an edge with datetime properties.
        final Edge e = g.addE("edge")
                .property("initD", initD)
                .property("initDT", initDT)
                .property("initODT", initODT)
                .property("initList", initList)
                .from(v1)
                .to(v2)
                .next();

        final Date addedD = getDate(2025, Calendar.SEPTEMBER, 22);
        final Date addedDT = getDate(2024, Calendar.FEBRUARY, 8, 9, 10, 0, 0);

        OffsetDateTime addedODT = OffsetDateTime.of(2023, 9, 8, 9, 10,
                11, 12, ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS);
        final List<Object> addedList = new ArrayList<>();
        addedList.add(addedD);
        addedList.add(addedDT);
        addedList.add(addedODT);

        // Add DateTime properties to an existing vertex.
        g.V(v1.id())
                .property("addedD", addedD)
                .property("addedDT", addedDT)
                .property("addedODT", addedODT)
                .property("addedList", addedList)
                .iterate();
        // Add DateTime properties to an existing edge.
        g.E(e.id())
                .property("addedD", addedD)
                .property("addedDT", addedDT)
                .property("addedODT", addedODT)
                .property("addedList", addedList)
                .iterate();

        // Test vertex datetime.
        // Newly created vertex with datetime properties.
        Assert.assertEquals(initD, g.V(v2.id()).values("initD").next());
        Assert.assertEquals(initDT, g.V(v2.id()).values("initDT").next());
        Assert.assertEquals(initODT, g.V(v2.id()).values("initODT").next());
        // List property with datetime values.
        Assert.assertEquals(initList, g.V(v2.id()).values("initList").next());
        // Added datetime properties to an existing vertex.
        Assert.assertEquals(addedD, g.V(v1.id()).values("addedD").next());
        Assert.assertEquals(addedDT, g.V(v1.id()).values("addedDT").next());
        Assert.assertEquals(addedODT, g.V(v1.id()).values("addedODT").next());
        // List property with datetime values.
        Assert.assertEquals(addedList, g.V(v1.id()).values("addedList").next());

        // Test edge datetime.
        // Newly created edge with datetime properties.
        Assert.assertEquals(initD, g.E(e.id()).values("initD").next());
        Assert.assertEquals(initDT, g.E(e.id()).values("initDT").next());
        Assert.assertEquals(initODT, g.E(e.id()).values("initODT").next());
        // List property with datetime values.
        Assert.assertEquals(initList, g.E(e.id()).values("initList").next());
        // Added datetime properties to an existing edge.
        Assert.assertEquals(addedD, g.E(e.id()).values("addedD").next());
        Assert.assertEquals(addedDT, g.E(e.id()).values("addedDT").next());
        Assert.assertEquals(addedODT, g.E(e.id()).values("addedODT").next());
        // List property with datetime values.
        Assert.assertEquals(addedList, g.E(e.id()).values("addedList").next());

        // Vertices datetime queries.
        List<Vertex> vResults = g.V().has("initD", initD).toList();
        Assert.assertEquals(1, vResults.size());
        vResults = g.V().has("addedDT", addedDT).toList();
        Assert.assertEquals(1, vResults.size());
        vResults = g.V().has("addedODT", addedODT).toList();
        Assert.assertEquals(1, vResults.size());
        vResults = g.V().has("addedODT", P.within(addedDT, addedODT)).toList();
        Assert.assertEquals(1, vResults.size());

        final Date noneExistingAddedDT = getDate(2019, Calendar.AUGUST, 15, 10, 30, 0, 0);

        vResults = g.V().has("addedDT", noneExistingAddedDT).toList();
        Assert.assertTrue(vResults.isEmpty());

        // Edges datetime queries.
        List<Edge> eResults = g.E().has("addedDT", addedDT).toList();
        Assert.assertEquals(1, eResults.size());

        final Date queryDT1 = getDate(2020, Calendar.JANUARY, 1);
        eResults = g.E().has("initDT", P.gt(queryDT1)).toList();
        Assert.assertEquals(1, eResults.size());

        final Date queryDT2 = getDate(2031, Calendar.JANUARY, 1);
        eResults = g.E().has("initDT", P.gt(queryDT2)).toList();
        Assert.assertEquals(0, eResults.size());

        final Date queryD = getDate(2025, Calendar.SEPTEMBER, 22);
        eResults = g.E().has("addedD", P.eq(queryD)).toList();
        Assert.assertEquals(1, eResults.size());

        eResults = g.E().has("addedD", P.within(addedD, addedDT)).toList();
        Assert.assertEquals(1, eResults.size());
        eResults = g.E().has("addedD", P.within(initD, initDT)).toList();
        Assert.assertEquals(0, eResults.size());

        // Test vertex datetime property properties only for in-memory run, not a remote graph
        g.V().hasLabel("person").property("name", "simon").property("age", "trente").iterate();
        g.V().hasLabel("person").properties("name")
                .property("date", initD)
                .property("dateTime", initDT)
                .property("offsetDateTime", initODT)
                .iterate();
        g.V().hasLabel("person").properties("age")
                .property("date", addedD)
                .property("dateTime", addedDT)
                .property("offsetDateTime", addedODT)
                .iterate();
        GraphTraversal<Vertex, ? extends Property<Object>> initDVps =
                g.V().properties().has("date", initD);
        Property<Object> name = initDVps.next();
        Assert.assertEquals("name", name.key());
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(initDVps.hasNext());
        GraphTraversal<Vertex, ? extends Property<Object>> addedDTVps =
                g.V().properties().has("dateTime", initDT);
        name = addedDTVps.next();
        Assert.assertEquals("name", name.key());
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(addedDTVps.hasNext());
        GraphTraversal<Vertex, ? extends Property<Object>> vertexProperties =
                g.V().properties().has("date");
        Property<Object> vertexProperty = vertexProperties.next();
        Assert.assertTrue((vertexProperty.key().equals("name") && vertexProperty.value().equals("simon")) ||
                (vertexProperty.key().equals("age") && vertexProperty.value().equals("trente")));
        vertexProperty = vertexProperties.next();
        Assert.assertTrue((vertexProperty.key().equals("name") && vertexProperty.value().equals("simon")) ||
                (vertexProperty.key().equals("age") && vertexProperty.value().equals("trente")));
        Assert.assertFalse(vertexProperties.hasNext());
    }

    public static Date getDate(int year, int month, int day) {
        return getDate(year, month, day, 0, 0, 0, 0);
    }

    public static Date getDate(int year, int month, int day, int hour, int minute, int second, int millisecond) {
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.set(Calendar.YEAR, year);
        // Starting at 0
        cal.set(Calendar.MONTH, month);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millisecond);
        return cal.getTime();
    }
}
