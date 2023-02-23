package com.aerospike.firefly.generator.identitygraphgenerator;

import org.apache.commons.cli.CommandLine;
import org.codehaus.plexus.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.firefly.generator.identitygraphgenerator.DataGenerator.parseCmdArgs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class DataGeneratorTest {
    static String dataDir;

    public static class CSVLine {
        public static class Keys {
            public static final String OUT_V = "~from";
            public static final String IN_V = "~to";
        }

        private final List<Object> line;
        private final List<String> header;

        private CSVLine(String line, String headerLine) {
            final List<String> header = parseHeader(headerLine);
            this.line = parseLine(header, line);
            this.header = header;
        }

        private static List<String> parseHeader(final String header) {
            final StringTokenizer st = new StringTokenizer(header, ",");
            final List<String> keys = new ArrayList<>();
            while (st.hasMoreTokens()) {
                keys.add(st.nextToken());
            }
            return keys;
        }

        enum CSVField {
            EMPTY
        }

        private List<Object> parseLine(List<String> header, String line) {
            StringTokenizer st = new StringTokenizer(line, ",", true);
            List<Object> results = new ArrayList<>();
            for (long i = 0; i < header.size(); i++) {
                String token = st.nextToken();
                if (token.equals(",")) {
                    // empty field
                    results.add(CSVField.EMPTY);
                } else {
                    results.add(token);
                    if (st.hasMoreTokens() && !Objects.equals(st.nextToken(), ",")) {
                        throw new RuntimeException("Expected comma after field");
                    }
                }
            }
            return results;
        }

        public Object getEntry(String key) {
            try {
                return line.get(header.indexOf(key));
            } catch (IndexOutOfBoundsException e) {
                throw new RuntimeException("Key not found: " + key);
            }
        }
    }
    @Before
    public void setUp() throws Exception {
        dataDir = Files.createTempDirectory("datagenerator").toAbsolutePath().toString();
    }
    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(dataDir));
    }

    public void mockMain(String[] args) {

        final CommandLine cmd = parseCmdArgs(args);
        final String numOfHouseholds = cmd.hasOption("h") ? cmd.getOptionValue("h") : "50";
        IdentityGenerator.Builder builder = IdentityGenerator.Builder.create();
        builder = builder.opsPerTransaction(500)
                .cmdLineArgs(cmd)
                .households(Integer.parseInt(numOfHouseholds))
                .accountsPerHousehold(10)
                .peoplePerHousehold(3)
                .devicesPerPerson(2);
        IdentityGenerator identityGenerator = builder.generate();
        identityGenerator.run();
    }

    @Test
    public void testGeneratedFileCount() throws IOException {
        assertTrue(Files.exists(Paths.get(dataDir)));
        mockMain(new String[]{"-e", "local", "-d", dataDir, "-h", "2", "-r","2", "-v", "0"});
        List<String> dirs = new ArrayList<>() {
            {
                add("vertices");
                add("edges");
            }
        };
        for (String dir : dirs) {
            try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve(dir))) {
                long count = path.filter(Files::isRegularFile).count();
                assertEquals(20, count);
            }
        }
    }

    @Test
    public void testGeneratedData() throws IOException {
        assertTrue(Files.exists(Paths.get(dataDir)));
        mockMain(new String[]{"-e", "local", "-d", dataDir});
        List<Long> vertexIdsFromWrittenVertices;
        try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve("vertices"))) {
            vertexIdsFromWrittenVertices = path.filter(Files::isRegularFile)
                    .flatMap(f -> {
                        try {
                            return Files.readAllLines(f).stream()
                                    .filter(line -> !line.startsWith("~"))
                                    .map(line -> line.split(",")[0])
                                    .map(Long::parseLong);
                        } catch (IOException e) {
                            e.printStackTrace();
                            throw new RuntimeException("Unable to read Files for Vertices in path = " + dataDir);
                        }
                    }).collect(Collectors.toList());
        }

        Set<Long> uniqueVertexIdsFromWrittenVertices = new HashSet<>(vertexIdsFromWrittenVertices);
        assertEquals(uniqueVertexIdsFromWrittenVertices.size(), vertexIdsFromWrittenVertices.size());

        List<Field> fields = new ArrayList<>(Arrays.asList(CSVLine.Keys.class.getDeclaredFields()));
        Set<Long> uniqueVertexIDs;
        for (Field field : fields) {
            try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve("edges"))) {
                uniqueVertexIDs = path.filter(Files::isRegularFile)
                    .flatMap(f -> {
                        try {
                            String header = Files.readAllLines(f).get(0);
                            return Files.readAllLines(f).stream()
                                .filter(line -> !line.startsWith("~"))
                                .map(line -> {
                                    CSVLine csvLine = new CSVLine(line, header);
                                    try {
                                        return Long.parseLong((String) csvLine.getEntry((String) field.get(this)));
                                    } catch (IllegalAccessException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                        } catch (IOException e) {
                            e.printStackTrace();
                            throw new RuntimeException("Unable to read Files for vertices in path = " + dataDir);
                        }
                    }).collect(Collectors.toSet());
            }

            uniqueVertexIDs.forEach(id -> {
                try{
                    assertTrue(vertexIdsFromWrittenVertices.contains(id));
                } catch (AssertionError e){
                    System.out.println("Vertex in id=" + id + "not found for edge");
                    throw new RuntimeException("Vertex In id=" + id + "not found for edge");
                }
            });
        }
    }

    @Test
    public void testGeneratedIDForVertexIsNotNull() throws IOException {
        assertTrue(Files.exists(Paths.get(dataDir)));
        mockMain(new String[]{"-e", "local", "-d", dataDir});
        long vertexCount;
        try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve("vertices"))) {
            vertexCount = path.filter(Files::isRegularFile)
                .map(f -> {
                    try {
                        return Files.readAllLines(f).size() - 1; // exclude header
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to read Files for vertices in testGeneratedIDForVertexIsNotNull in path = " + dataDir);
                    }
            }).reduce(0, Integer::sum);
        }

        List<Long> vertexIdsFromWrittenVertices;
        try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve("vertices"))) {
            vertexIdsFromWrittenVertices = path.filter(Files::isRegularFile)
                .flatMap(f -> {
                    try {
                        return Files.readAllLines(f).stream()
                                .filter(line -> !line.startsWith("~"))
                                .map(line -> line.split(",")[0])
                                .map(Long::parseLong);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to read Files for vertices in testGeneratedIDForVertexIsNotNull in path = " + dataDir);
                    }
            }).collect(Collectors.toList());
        }
        assertEquals(vertexCount, vertexIdsFromWrittenVertices.size());
    }

    @Test
    public void testGeneratedIDForEdgesIsNotNull() throws IOException {
        assertTrue(Files.exists(Paths.get(dataDir)));
        mockMain(new String[]{"-e", "local", "-d", dataDir});
        long edgeCount;
        try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve("edges"))) {
            edgeCount = path.filter(Files::isRegularFile)
                .map(f -> {
                    try {
                        return Files.readAllLines(f).size() - 1; // exclude header
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to read Files for edges in testGeneratedIDForEdgesIsNotNull in path = " + dataDir);
                    }
            }).reduce(0, Integer::sum);
        }
        List<Long> edgeIdsFromWrittenEdges;
        try(Stream<Path> path = Files.walk(Paths.get(dataDir).resolve("edges"))) {
            edgeIdsFromWrittenEdges = path.filter(Files::isRegularFile)
                .flatMap(f -> {
                    try {
                        return Files.readAllLines(f).stream()
                                .filter(line -> !line.startsWith("~"))
                                .map(line -> line.split(",")[0])
                                .map(Long::parseLong);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Unable to read Files for edges in testGeneratedIDForEdgesIsNotNull in path = " + dataDir);
                    }
            }).collect(Collectors.toList());
        }
        assertEquals(edgeCount, edgeIdsFromWrittenEdges.size());
    }
}