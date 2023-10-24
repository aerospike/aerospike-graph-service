package com.aerospike.firefly;

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
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.Main.testMain;

public class TestMain {

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
        Assert.assertEquals(979, averageVertexRecordSize);
        Assert.assertEquals(480, averageEdgeRecordSize);
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
        Assert.assertEquals(979, outputYaml.averageVertexRecordSize.longValue());
        Assert.assertEquals(480, outputYaml.averageEdgeRecordSize.longValue());
        Assert.assertEquals(outputYaml.vertexRecordCount.longValue(), outputYaml.totalSindexEntries.longValue());
    }

    @Test
    public void testCLIInvalidOutputFilePath() {
        final List<Exception> exceptions = Main.testMain(new String[]{"src/test/resources/test_schema.yaml", "/foo/src/test/resources/test_schema.json"});
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
