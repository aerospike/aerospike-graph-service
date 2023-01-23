package com.aerospike.generator.identitygenerator;

import com.opencsv.CSVWriter;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * IdentityGenerator is Runnable and multiple independent threads/workers can be spawned each executing their own IdentityGenerator instance.
 * Given the nature of the underlying graph structure, it is not necessary for the individual workers to have knowledge/reference to
 * the subgraphs of parallel IdentityGenerators. Such a structure allows for an embarrassingly parallel graph generator.
 *
 * IdentityGenerator.Builder is used to create the Runnable.
 *
 * <pre><code>
 * IdentityGenerator ig =
 *   IdentityGenerator.Builder.create()
 *     .households(100_000)         // primary loop counter
 *     .accountsPerHousehold(2)     // gaussian sampled
 *     .peoplePerHousehold(6)       // gaussian sampled
 *     .devicesPerPerson(2)         // gaussian sampled
 *     .opsPerTransaction(1000)     // auto-commit handler
 *     .generate(graph);
 * new Thread(ig).start();
 * </code></pre>
 *
 */
public class IdentityGenerator implements Runnable {

    // IDENTITY GRAPH SCHEMA //
    //////// PERSON VERTEX ////////
    private static final String PERSON = "Person";
    private static final String EMAIL = "email";
    private static final String SSN = "ssn";
    private static final String FIRST_NAME = "firstName";
    private static final String LAST_NAME = "lastName";
    private static final String PHONE = "phone";
    //////// HOUSEHOLD VERTEX ////////
    private static final String HOUSEHOLD = "Hosehold";
    private static final String STREET = "street";
    private static final String CITY = "city";
    private static final String STATE = "state";
    private static final String ZIPCODE = "zipcode";
    //////// ACCOUNT VERTEX ////////
    private static final String ACCOUNT = "Account";
    private static final String NUMBER = "number";
    //////// DEVICE VERTEX ////////
    private static final String DEVICE = "Device";
    private static final String MAC_ADDRESS = "macAddress";
    private static final String MAKE = "make";
    private static final String MODEL = "model";
    ////////////////////////////////////////////////////////
    //////// EXPLICIT EDGES ////////
    private static final String HOLDS = "holds";             // Person--holds-->Account
    private static final String PART_OF = "partOf";         // Person--partOf-->Household
    private static final String OWNS = "owns";              // Person--owns-->Device
    private static final String SUB_ACCOUNT = "subAccount"; // Account--subAccount-->Account
    //////// IMPLICIT EDGES ////////
    private static final String HAS_COHABITATOR = "hasCohabitator"; // Person--hasCohabitator-->Person
    private static final String HAS_ACCOUNT = "hasAccount";         // Household--hasAccount-->Account
    private static final String CONTAINS = "contains";              // Household--contains-->Device
    private static final String FOR_DEVICES = "forDevice";         // Account--forDevice-->Device

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static final List<String> LETTERS = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
    private static final List<String> DOMAINS = List.of(".com", ".net", ".org", ".edu", ".biz");
    private static final List<String> MAKES = List.of("Apple", "Google", "Samsung", "Nokia", "Roku", "Microsoft");

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private List<String> verticesHeaders = Arrays.asList("~id", "~label");
    private LinkedHashSet<String> edgesHeaders = new LinkedHashSet<>(Arrays.asList("~id", "~label", "~from", "~to")); // INVID = FROM & OUTVID = TO
    private long NO_OF_ROWS = 10000;
    private HashMap<String, HashMap<String, Object>> graphMap = new HashMap<>();
    private final Graph graph;
    private final Builder builder;
    private final Random random = new Random();
    private Logger LOG;
    private final IdentityGenerator.CsvWriter csvWriter;
    private Future future;

    private IdentityGenerator(final Builder builder) {
        this.builder = builder;
        this.graph = builder.graph;
        this.csvWriter = new IdentityGenerator.CsvWriter("./datageneratoroutput/identitygenerator", builder);
        this.LOG = builder.logger;
    }

    /**
     * The method evaluated by the thread.
     */
    @Override
    public void run() {
        LOG.info("Generating subgraph for worker " + this.builder.id + " " + this.builder);
        try {
            for (int i = 0; i < builder.numberOfHouseholds; i++) {
                final Vertex household;
                household = this.createHousehold("vertex/Household", "household");
                final long numberOfPeople = this.getGaussian(builder.peoplePerHousehold, 2); // +/- 2 from the mean
                final long numberOfAccounts = this.getGaussian(builder.accountsPerHousehold, 1);
                final List<Vertex> accounts = new ArrayList<>();
                for (int j = 0; j < numberOfAccounts; j++) {
                    final Vertex account = this.createAccount("vertex/Account", "account");
                    if (accounts.size() > 1) {
                        final Vertex rootAccount = accounts.get(this.random.nextInt(accounts.size() - 1));
                        this.createSubAccount(rootAccount, account, "edges/SubAccount", "subaccount");
                    }
                    accounts.add(account);
                }
                for (int j = 0; j < numberOfPeople; j++) {
                    final Vertex person = this.createPerson("vertex/Person", "person");
                    if (accounts.size() > 0) this.createHolds(person, accounts.remove(0), "edges/Holds", "holds");
                    this.createPartOf(person, household, "edges/PartOf", "partof");
                    final long numberOfDevices = this.getGaussian(builder.devicesPerPerson, 2);
                    for (int k = 0; k < numberOfDevices; k++) {
                        final Vertex device = this.createDevice("vertex/Device", "device");
                        this.createOwns(person, device, "edges/Owns", "owns");
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setFuture(Future future) {
        this.future = future;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Edge createSubAccount(final Vertex account, final Vertex subAccount, String dir, String fileName) {
        final Edge edge = account.addEdge(SUB_ACCOUNT, subAccount);  // properties on edges?
        generateAndWriteEdgeData(account, subAccount, edge, dir, fileName);
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Edge createHolds(final Vertex person, final Vertex account, String dir, String fileName) {
        final Edge edge = person.addEdge(HOLDS, account);  // properties on edges?
        generateAndWriteEdgeData(person, account, edge, dir, fileName);
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Edge createOwns(final Vertex person, final Vertex device, String dir, String fileName) {
        final Edge edge = person.addEdge(OWNS, device);  // properties on edges?
        generateAndWriteEdgeData(person, device, edge, dir, fileName);
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Edge createPartOf(final Vertex person, final Vertex household, String dir, String fileName) throws IOException {
        final Edge partOf = person.addEdge(PART_OF, household);
        generateAndWriteEdgeData(person, household, partOf, dir, fileName);
        return partOf;
    }

    public Vertex createHousehold(String dir, String fileName) throws IOException {
        final Vertex household = this.graph.addVertex(
                T.label, HOUSEHOLD,
                STREET, this.createStreet(),
                CITY, this.createName(5),
                STATE, this.createName(2, 0),
                ZIPCODE, this.createNumber(5));
        generateAndWriteVertexData(household, dir, fileName);
        return household;
    }

    public Vertex createAccount(String dir, String fileName) throws IOException {
        final Vertex account = this.graph.addVertex(
                T.label, ACCOUNT,
                NUMBER, UUID.randomUUID().toString());
        generateAndWriteVertexData(account, dir,fileName);
        return account;
    }

    public void generateAndWriteVertexData(Vertex vertex, String dir, String fileName){
        final MutablePair<String[], String[]> pair = generateVertexData(vertex);
        generateAndWriteData(pair, dir, fileName);
    }

    public void generateAndWriteEdgeData(Vertex from, Vertex to, Edge edge, String dir, String fileName){
        final MutablePair<String[], String[]> pair = generateEdgeData(from, to, edge);
        generateAndWriteData(pair, dir, fileName);
    }

    public void generateAndWriteData(MutablePair<String[], String[]> pair, String dir, String fileName) {
        int fileCount = 0;
        if (graphMap.containsKey(fileName))
            fileCount = (int)graphMap.get(fileName).get("fileCount");
        Integer countOfRecords = populateGraphMap(fileName, pair, fileCount);
        if ( countOfRecords == NO_OF_ROWS) {
            this.csvWriter.writeDataMapToCSV(this.graphMap, dir, fileName, fileCount);
            HashMap<String, Object> objectPropertyMap = new HashMap<>();
            objectPropertyMap.put("fileCount", fileCount + 1);
            HashSet<String[]> schemaSet = new HashSet<>();
            schemaSet.add(pair.left);
            objectPropertyMap.put("schema", schemaSet);
            objectPropertyMap.put("data", new ArrayList<>());
            graphMap.put(fileName, objectPropertyMap);
        }
    }

    public Vertex createPerson(String dir, String fileName) throws IOException {
        final Vertex person = this.graph.addVertex(
                T.label, PERSON,
                FIRST_NAME, this.createName(1, 0).toUpperCase() + this.createName(5),
                LAST_NAME, this.createName(1, 0).toUpperCase() + this.createName(10),
                SSN, createNumber(9),
                EMAIL, createEmail(10),
                PHONE, createNumber(10));
        generateAndWriteVertexData(person, dir, fileName);
        return person;
    }

    public Vertex createDevice(String dir, String fileName) {
        final Vertex device = this.graph.addVertex(
                T.label, DEVICE,
                MAC_ADDRESS, UUID.randomUUID().toString(),
                MAKE, MAKES.get(this.random.nextInt(MAKES.size() - 1)),
                MODEL, createName(10));
        generateAndWriteVertexData(device, dir, fileName);
        return device;
    }

    public synchronized Integer populateGraphMap(String fileName, MutablePair<String[], String[]> pair, int fileCount) {
        HashMap<String, Object> objectPropertyMap;
        if (!graphMap.containsKey(fileName)) {
            objectPropertyMap = new HashMap<>();
            //update file fileCount to be appended to the output file
            objectPropertyMap.put("fileCount", fileCount);
            HashSet<String[]> schemaSet = new HashSet<>();
            schemaSet.add(pair.left);
            objectPropertyMap.put("schema", schemaSet);
            objectPropertyMap.put("data", new ArrayList<>());
            graphMap.put(fileName, objectPropertyMap);
        }
        else objectPropertyMap = graphMap.get(fileName);

        ArrayList<String[]> dataList = (ArrayList<String[]>)objectPropertyMap.get("data");
        dataList.add(pair.right);
        objectPropertyMap.put("data", dataList);
        graphMap.put(fileName, objectPropertyMap);
        return dataList.size();
    }
    public synchronized MutablePair<String[], String[]> generateVertexData(Vertex vertex) {
        ArrayList<String> vertexheaders = (ArrayList<String>) verticesHeaders.stream().collect(Collectors.toList());
        vertexheaders.addAll(vertex.keys());
        ArrayList<String> data =new ArrayList<>();
        data.add(vertex.id().toString());
        data.add(vertex.label());

        for (String key : vertex.keys())
            data.add(vertex.value(key).toString());

        return new MutablePair<>(Arrays.copyOf(vertexheaders.toArray(), vertexheaders.toArray().length, String[].class), Arrays.copyOf(data.toArray(), data.toArray().length, String[].class));
    }

    public synchronized MutablePair<String[], String[]> generateEdgeData(Vertex from, Vertex to, Edge edge) {
        edgesHeaders.addAll(edge.keys());
        ArrayList<String> data =new ArrayList<>();
        data.add(edge.id().toString());
        data.add(edge.label());
        data.add(from.id().toString());
        data.add(to.id().toString());

        for (String key : edge.keys())
            data.add(edge.value(key).toString());

        return new MutablePair<>(Arrays.copyOf(edgesHeaders.toArray(), edgesHeaders.toArray().length, String[].class), Arrays.copyOf(data.toArray(), data.toArray().length, String[].class));
    }

    /**
     * For future use when needing to add arbitrary data to 'bulk up' the data set artificially
     */
    public <T extends Element> T createRandomProperty(final T element, final String key, final Class type) {
        if (type.equals(Long.class))
            element.property(key, Math.abs(this.random.nextLong()));
        else if (type.equals(String.class))
            element.property(key, this.createName(10));
        else if (type.equals(Boolean.class))
            element.property(key, this.random.nextBoolean());
        else
            throw new IllegalArgumentException("The only types supported are String, Long, and Boolean: " + type.getSimpleName());
        return element;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public String createName(final int meanLength, final int variance) {
        String name = "";
        for (int i = 0; i < getGaussian(meanLength, variance); i++) {
            name = name + LETTERS.get(this.random.nextInt(LETTERS.size() - 1));
        }
        return name;
    }
    public String createName(final int meanLength) {

        return createName(meanLength, 2);
    }

    public Long createNumber(final int length) {
        String number = "";
        for (int i = 0; i < length; i++) {
            number = number + this.random.nextInt(9);
        }
        return Long.valueOf(number);
    }

    public String createEmail(final int meanLength) {
        return this.createName(meanLength) + "@" + this.createName(5) + DOMAINS.get(this.random.nextInt(DOMAINS.size() - 1));
    }

    public String createStreet() {
        String street = "";
        for (int i = 0; i < this.getGaussian(6, 2); i++) {
            street = street + this.random.nextInt(9);
        }
        return street + " " + this.createName(10);
    }

    private final long getGaussian(int mean, int variance) {
        return Math.round(mean + this.random.nextGaussian() * variance);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class CsvWriter {
        private String path;

        public CsvWriter(String path, final IdentityGenerator.Builder builder) {
            this.path = path;
        }

        public void writeDataMapToCSV(HashMap<String, HashMap<String,Object>> map, String dir, String fileName, int count) {
            java.io.File file = new java.io.File(path + "/" + dir + "/" + fileName);
            file.getParentFile().mkdirs();
            try (CSVWriter writer1 = new CSVWriter(
                    new FileWriter(path + "/" + dir + "/" + fileName + "_" + count + ".csv"),
                    ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {
                // write the schema at the top of file
                writer1.writeAll((HashSet<String[]>)map.get(fileName).get("schema"));
                // write the data
                writer1.writeAll((ArrayList<String[]>)map.get(fileName).get("data"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class Builder {

        protected Graph graph;
        protected Logger logger = LoggerFactory.getLogger(IdentityGenerator.class);
        protected int numberOfHouseholds;
        protected int peoplePerHousehold;
        protected int devicesPerPerson;
        protected int accountsPerHousehold;
        protected String id = UUID.randomUUID().toString();
        protected int ops = 1000;

        public static Builder create() {
            return new Builder();
        }

        public Builder workerId(final String id) {
            this.id = id;
            return this;
        }

        public Builder opsPerTransaction(final int ops) {
            this.ops = ops;
            return this;
        }

        public Builder logger(final Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder households(final int numberOfHouseholds) {
            this.numberOfHouseholds = numberOfHouseholds;
            return this;
        }

        public Builder accountsPerHousehold(final int meanAccountsPerHousehold) {
            this.accountsPerHousehold = meanAccountsPerHousehold;
            return this;
        }

        public Builder peoplePerHousehold(final int meanPeoplePerHousehold) {
            this.peoplePerHousehold = meanPeoplePerHousehold;
            return this;
        }

        public Builder devicesPerPerson(final int meanDevicesPerPerson) {
            this.devicesPerPerson = meanDevicesPerPerson;
            return this;
        }

        public IdentityGenerator generate(final Graph graph) {
            this.graph = graph;
            return new IdentityGenerator(this);
        }

        public String toString() {
            return "IdentityGraph.Builder[" +
                    "id:" + this.id +
                    ",numberOfHouseholds:" + this.numberOfHouseholds +
                    ",peoplePerHousehold:" + this.peoplePerHousehold +
                    ",devicesPerHousehold:" + this.devicesPerPerson +
                    ",opsPerTransaction:" + this.ops + "]";
        }
    }
}
