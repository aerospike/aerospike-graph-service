package com.aerospike.firefly;

import com.aerospike.firefly.sizing.SizingToolPlugin;
import com.aerospike.firefly.util.PluginUtil;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.sizing.SizingToolMain.testMain;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestSizingTool {

    @Test
    public void testCLIJson() throws IOException {
        final Path currentRelativePath = Paths.get("");
        final String s = currentRelativePath.toAbsolutePath().toString();
        final List<Exception> exceptions = testMain(new String[]{"src/test/resources/test_schema.yaml", s + "/src/test/resources/test_schema_output.json"});
        Assert.assertEquals(0, exceptions.size());
        final AtomicReference<String> json = new AtomicReference<>("");
        Files.lines(Path.of(s + "/src/test/resources/test_schema_output.json")).forEach(l -> json.set(json.get() + l));
        final JSONObject jsonObject = new JSONObject(json.get());
        final Object vertexRecordCount = jsonObject.get("vertexRecordCount");
        final Object edgeRecordCount = jsonObject.get("edgeRecordCount");
        final Object averageVertexRecordSize = jsonObject.get("averageVertexRecordSize");
        final Object averageEdgeRecordSize = jsonObject.get("averageEdgeRecordSize");
        final Object totalSindexEntries = jsonObject.get("totalSindexEntries");
        Assert.assertEquals(100, vertexRecordCount);
        Assert.assertEquals(2000 / 10, edgeRecordCount);
        Assert.assertEquals(totalSindexEntries, vertexRecordCount);
        Assert.assertEquals(1899, averageVertexRecordSize);
        Assert.assertEquals(2060, averageEdgeRecordSize);
    }


    public static class OutputYaml {
        public Long vertexRecordCount;
        public Long edgeRecordCount;
        public Long averageVertexRecordSize;
        public Long averageEdgeRecordSize;
        public Long totalSindexEntries;

        public OutputYaml() {
        }
    }

    @Test
    public void testGraph() throws Exception {
        try (final Graph graph = TinkerGraph.open()) {
            final GraphTraversalSource g = graph.traversal();

            g.addV().property(T.id, "~metadata").
                    property("replicationFactor", 2).
                    property("edgePackSize", 10).
                    property("edgeCacheSize", 100).iterate();

            final Vertex person = g.addV("person").
                    property("person.count", 100L).
                    property("name", "String").
                    property("name.valueSize", 10L).
                    property("name.sindexed", false).
                    property("age", "Long").next();

            g.addE("KNOWS").from(person).to(person).
                    property("KNOWS.count", 100L).
                    iterate();

            g.addE("LIKES").from(person).to(person).
                    property("since", "Long").
                    property("LIKES.count", 100L).
                    property("rating", "Double").iterate();

            g.addE("LIKES2").from(person).to(person).
                    property("since", "Long").
                    property("LIKES2.count", 100).
                    property("rating", "Double").iterate();


            PluginUtil.loadPlugin(SizingToolPlugin.class.getName(), new MapConfiguration(Map.of()), graph);
            Object f = g.call("sizing-tool").next();
            System.out.println("f = " + f);
        }
    }

    @Test
    public void testFullExample() throws Exception {
        try (final Graph graph = TinkerGraph.open()) {
            PluginUtil.loadPlugin(SizingToolPlugin.class.getName(), new MapConfiguration(Map.of()), graph);
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();

            // Set replicationFactor of 2 and enable the vertexLabelSindex since this application will be querying by vertex label alone.
            // maxEdgeCacheSize and edgePackSize are left as their defaults.
            g.addV().property(T.id, "~metadata").
                    property("replicationFactor", 2).
                    property("vertexLabelSindex", true).iterate();

            // Groomers have names and phone numbers. In this grooming salon we have 10 individual Groomers.
            // The name of the Groomer is secondary indexed so it can be looked up very fast.
            Vertex groomer = g.addV("Groomer").
                    property("Groomer.count", 10).
                    property("name", "String").
                    property("name.sindexed", true).
                    property("name.valueSize", 15).
                    property("phone", "String").
                    property("phone.valueSize", 12).
                    next();

            // Clients have names and phone numbers. Each Groomer has about 100 Clients, leading to 1000 Clients in total.
            // No secondary indexes are created on client data.
            Vertex client = g.addV("Client").
                    property("Client.count", 1000).
                    property("name", "String").
                    property("name.valueSize", 15).
                    property("phone", "String").
                    property("phone.valueSize", 12).
                    next();

            // Pets have a name and a breed. Each client has on average 1.5 pets, leading to 1500 pets in total.
            // No secondary indexes are created on pet data.
            Vertex pet = g.addV("Pet").
                    property("Pet.count", 1500).
                    property("name", "String").
                    property("name.valueSize", 15).
                    property("breed", "String").
                    property("breed.valueSize", 10).
                    next();

            // Every pet has an appointment with a groomer scheduled whenever their last appointment ends, leading to 1500 appointments.
            // Each appointment has a date, a time, and an estimatedAppointmentLength, where the date is a String, and the time and estimated appointment time are Integers.
            // No secondary indexes are created on appointment data.
            // The label Appt is used to abbreviate Appointment.
            Vertex appointment = g.addV("Appt").
                    property("Appt.count", 1500).
                    property("date", "String").
                    property("date.valueSize", 10).
                    property("time", "Integer").
                    property("estimatedAppointmentLength", "Integer").
                    next();

            // Groomers provide services. Each Groomer provides 5 services, leading to 50 services in total.
            // A service has a serviceType.
            // No secondary indexes are created on service data.
            Vertex service = g.addV("Service").
                    property("Service.count", 50).
                    property("serviceType", "String").
                    property("serviceType.valueSize", 25).
                    next();

            // Note - since we do not use the return value of edges, iterate() is used to terminate the query instead of next.

            // Clients are connected to Groomers with a CLIENT_OF edge. Each Client has a CLIENT_OF edge to 1 Groomer.
            // Since there are 1000 Clients, there are 1000 CLIENT_OF edges.
            // Client edges detail the date the client-groomer relationship was established with a date String.
            g.addE("CLIENT_OF").from(client).to(groomer).
                    property("CLIENT_OF.count", 1000).
                    property("date", "String").
                    property("date.valueSize", 10).
                    iterate();

            // Pets are connected to Clients with a OWNER edge. Each Pet has an OWNER edge to 1 Client.
            // Since there are 1500 Pets, there are 1500 OWNER edges.
            g.addE("OWNER").from(pet).to(client).
                    property("OWNER.count", 1500).
                    iterate();

            // Appointments are connected to Groomers with a WITH_GROOMER edge. Each Appointment has a WITH_GROOMER edge to 1 Groomer.
            // Since there are 1500 Appointments, there are 1500 WITH_GROOMER edges.
            g.addE("WITH_GROOMER").from(appointment).to(groomer).
                    property("WITH_GROOMER.count", 1500).
                    iterate();

            // Appointments are connected to Pets with a WITH_PET edge. Each Appointment has a WITH_PET edge to 1 Pet.
            // Since there are 1500 Appointments, there are 1500 WITH_PET edges.
            g.addE("WITH_PET").from(appointment).to(pet).
                    property("WITH_PET.count", 1500).
                    iterate();

            // Appointments are connected to Services with a SERVICE edge. Each Appointment has SERVICE edges to many Services.
            // On average, 3 services are done per Appointment, leading to 4500 SERVICE edges.
            g.addE("SERVICE").from(appointment).to(service).
                    property("SERVICE.count", 4500).
                    iterate();

            // Groomers are connected to Services with a PROVIDES_SERVICE edge. Each Groomer has a PROVIDES_SERVICE edge to many Services.
            // On average, 10 services are provided by each Groomer, leading to 100 PROVIDES_SERVICE edges.
            g.addE("PROVIDES_SERVICE").from(groomer).to(service).
                    property("PROVIDES_SERVICE.count", 100).
                    iterate();

            g.call("sizing-tool").next();
        }
    }

    @Test
    public void testCLIYaml() throws IOException {
        final Path currentRelativePath = Paths.get("");
        final String s = currentRelativePath.toAbsolutePath().toString();
        final List<Exception> exceptions = testMain(new String[]{"src/test/resources/test_schema.yaml", s + "/src/test/resources/test_schema_output.yaml"});
        Assert.assertEquals(0, exceptions.size());
        final File file = new File(s + "/src/test/resources/test_schema_output.yaml");
        final String yamlText = Files.readAllLines(file.toPath(), Charset.defaultCharset()).stream().reduce("", (a, b) -> a + "\n" + b);
        final Yaml yaml = new Yaml(new Constructor(OutputYaml.class, new LoaderOptions()));
        final OutputYaml outputYaml = yaml.load(yamlText);
        Assert.assertEquals(100, outputYaml.vertexRecordCount.longValue());
        Assert.assertEquals(2000 / 10, outputYaml.edgeRecordCount.longValue());
        Assert.assertEquals(1899, outputYaml.averageVertexRecordSize.longValue());
        Assert.assertEquals(2060, outputYaml.averageEdgeRecordSize.longValue());
        Assert.assertEquals(outputYaml.vertexRecordCount.longValue(), outputYaml.totalSindexEntries.longValue());
    }

    @Test
    public void testCLIInvalidOutputFilePath() {
        final List<Exception> exceptions = testMain(new String[]{"src/test/resources/test_schema.yaml", "/foo/src/test/resources/test_schema.json"});
        Assert.assertEquals(exceptions.size(), 1);
        Assert.assertEquals("Cannot create file: /foo/src/test/resources/test_schema.json.", exceptions.get(0).getMessage());
    }

    @Test
    public void testCLIInvalidOutputFileType() {
        final Path currentRelativePath = Paths.get("");
        final String s = currentRelativePath.toAbsolutePath().toString();
        final List<Exception> exceptions = testMain(new String[]{"src/test/resources/test_schema.yaml", s + "/src/test/resources/test_schema.jsn"});
        Assert.assertEquals(exceptions.size(), 1);
        Assert.assertEquals("Invalid output file type: " + s + "/src/test/resources/test_schema.jsn. File should end with '.yaml', '.json', or '.csv'.", exceptions.get(0).getMessage());
    }

    @Test
    public void testCLIInvalidOutputIsDirectory() {
        final Path currentRelativePath = Paths.get("");
        final String s = currentRelativePath.toAbsolutePath().toString();
        final List<Exception> exceptions = testMain(new String[]{"src/test/resources/test_schema.yaml", s + "/src/test/resources/"});
        Assert.assertEquals(exceptions.size(), 1);
        Assert.assertEquals("File path is directory: " + s + "/src/test/resources/.", exceptions.get(0).getMessage());
    }
}
