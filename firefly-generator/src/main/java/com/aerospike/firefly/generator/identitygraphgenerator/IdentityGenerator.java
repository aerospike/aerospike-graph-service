package com.aerospike.firefly.generator.identitygraphgenerator;


import com.aerospike.firefly.generator.identitygraphgenerator.beans.Vertex;
import com.aerospike.firefly.generator.identitygraphgenerator.beans.edges.Edge;
import com.aerospike.firefly.generator.identitygraphgenerator.beans.vertices.Account;
import com.aerospike.firefly.generator.identitygraphgenerator.beans.vertices.Device;
import com.aerospike.firefly.generator.identitygraphgenerator.beans.vertices.Household;
import com.aerospike.firefly.generator.identitygraphgenerator.beans.vertices.Person;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.opencsv.CSVWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * IdentityGenerator is Runnable and multiple independent threads/workers can be spawned each executing their own IdentityGenerator instance.
 * Given the nature of the underlying graph structure, it is not necessary for the individual workers to have knowledge/reference to
 * the subgraphs of parallel IdentityGenerators. Such a structure allows for an embarrassingly parallel graph generator.
 * <p>
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
    private int numberOfRecordsPerFile = 100;
    private int variance = 2;
    private final HashMap<String, HashMap<String, Object>> graphMap = new HashMap<>();
    private final Builder builder;
    private final Random random = new Random();
    private final Logger LOG;
    private final IdentityGenerator.CsvWriter csvWriter;
    private static String ENV = "aws";
    //Set default path in local run mode
    private static String path = "./datagenerator";
    private static String bucket;
    private static AmazonS3 S3_CLIENT;
    private static final AtomicLong i = new AtomicLong(0);

    private final HashMap<String, MutablePair<ByteArrayOutputStream, OutputStreamWriter>> streamMap = new HashMap<>();

    private IdentityGenerator(final Builder builder) {
        this.builder = builder;
        CommandLine cmd = this.builder.cmd;
        this.LOG = builder.logger;
        ENV = cmd.hasOption("e") ? cmd.getOptionValue("e") : ENV;
        path = cmd.hasOption("d") ? cmd.getOptionValue("d") : path;
        numberOfRecordsPerFile = cmd.hasOption("r") ? Integer.parseInt(cmd.getOptionValue("r")) : numberOfRecordsPerFile;
        variance = cmd.hasOption("v") ? Integer.parseInt(cmd.getOptionValue("v")) : variance;
        if (ENV.equals("aws")) {
            bucket = cmd.getOptionValue("b");
            String accessKey = cmd.getOptionValue("a");
            String secretKey = cmd.getOptionValue("s");
            final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setProtocol(Protocol.HTTP);
            S3_CLIENT = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withClientConfiguration(clientConfig).build();
        }
        this.csvWriter = new IdentityGenerator.CsvWriter(path, streamMap);
    }

    /**
     * The method evaluated by the thread.
     */
    @Override
    public void run() {
        LOG.info("Generating subgraph for worker " + this.builder.id + " " + this.builder);
        Vertex newAccount = new Account();
        Vertex newDevice = new Device();
        Vertex newHouseHold = new Household();
        Vertex newPerson = new Person();
        if (builder.numberOfEdgeProperties > 0) {
            IntStream.range(0, builder.numberOfEdgeProperties - 1).forEach(i -> edgesHeaders.add(String.valueOf(i)));
        }
        try {
            for (int i = 0; i < builder.numberOfHouseholds; i++) {
                final Vertex household = this.createHousehold(newHouseHold, "vertices/Household", "household");
                // default variance set to 2.
                final long numberOfPeople = this.getGaussian(builder.peoplePerHousehold, variance); // +/- 2 from the mean
                final long numberOfAccounts = this.getGaussian(builder.accountsPerHousehold, variance);
                final List<Vertex> accounts = new ArrayList<>();
                for (int j = 0; j < numberOfAccounts; j++) {
                    final Vertex account = this.createAccount(newAccount, "vertices/Account", "account");
                    if (accounts.size() > 1) {
                        final Vertex rootAccount = accounts.get(this.random.nextInt(accounts.size() - 1));
                        this.createSubAccountNew(rootAccount,
                                account,
                                "edges/SubAccount",
                                builder.numberOfEdgeProperties,
                                builder.edgePropertyValueLength,
                                "subaccount");
                    }
                    accounts.add(account);
                }
                for (int j = 0; j < numberOfPeople; j++) {
                    final Vertex person = this.createPerson(newPerson, "vertices/Person", "person");
                    if (accounts.size() > 0)
                        this.createHoldsNew(person,
                                accounts.remove(0),
                                builder.numberOfEdgeProperties,
                                builder.edgePropertyValueLength,
                                "edges/Holds",
                                "holds");
                    this.createPartOfNew(person,
                            household,
                            "edges/PartOf",
                            builder.numberOfEdgeProperties,
                            builder.edgePropertyValueLength,
                            "partof");
                    final long numberOfDevices = this.getGaussian(builder.devicesPerPerson, variance);
                    for (int k = 0; k < numberOfDevices; k++) {
                        final Vertex device = this.createDevice(newDevice, "vertices/Device", "device");
                        this.createOwnsNew(person,
                                device,
                                "edges/Owns",
                                builder.numberOfEdgeProperties,
                                builder.edgePropertyValueLength,
                                "owns");
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Error in running data generator. " + e.getMessage());
            throw new RuntimeException(e);
        }
        //Write remaining file stream to output files
        for (Map.Entry<String, MutablePair<ByteArrayOutputStream, OutputStreamWriter>> entry : streamMap.entrySet()) {
            String fileName = entry.getKey();
            if (ENV.equals("aws"))
                this.csvWriter.flushStreamToS3(entry.getValue().left.toByteArray(), fileName);
            else
                this.csvWriter.flushStreamToFile(entry.getValue().left.toByteArray(), fileName);
        }
        LOG.info("Generating data for firefly graph done..");
    }

    public String[] createRandomPropertyKeyValues(int count, int valueSize) {
        List<String> keyValues = new ArrayList<>();
        for (int i = 0; i < count; i += 1) {
            keyValues.add(String.valueOf(i));
            keyValues.add(RandomStringUtils.randomAlphanumeric(valueSize));
        }
        return keyValues.toArray(new String[0]);
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void createSubAccountNew(final Vertex account,
                                    final Vertex subAccount,
                                    final String dir,
                                    final int numberOfEdgeProperties,
                                    final int edgePropertyValueLength,
                                    final String fileName) throws IOException {
        final Edge edge = new Edge(i.getAndIncrement(), SUB_ACCOUNT, account.getId(), subAccount.getId(),
                createRandomPropertyKeyValues(numberOfEdgeProperties, edgePropertyValueLength));
        generateAndWriteEdgeData(account, subAccount, edge, dir, fileName);
    }

    public void createHoldsNew(final Vertex person,
                               final Vertex account,
                               final int numberOfEdgeProperties,
                               final int edgePropertyValueLength,
                               final String dir,
                               final String fileName) throws IOException {
        final Edge edge = new Edge(i.getAndIncrement(), HOLDS, person.getId(), account.getId(),
                createRandomPropertyKeyValues(numberOfEdgeProperties, edgePropertyValueLength));
        generateAndWriteEdgeData(person, account, edge, dir, fileName);
    }

    public void createOwnsNew(final Vertex person,
                              final Vertex device,
                              final String dir,
                              final int numberOfEdgeProperties,
                              final int edgePropertyValueLength,
                              final String fileName) throws IOException {
        final Edge edge = new Edge(i.getAndIncrement(), OWNS, person.getId(), device.getId(),
                createRandomPropertyKeyValues(numberOfEdgeProperties, edgePropertyValueLength));
        generateAndWriteEdgeData(person, device, edge, dir, fileName);
    }

    public void createPartOfNew(final Vertex person,
                                final Vertex household,
                                final String dir,
                                final int numberOfEdgeProperties,
                                final int edgePropertyValueLength,
                                final String fileName) throws IOException {
        final Edge edge = new Edge(i.getAndIncrement(), PART_OF, person.getId(), household.getId(),
                createRandomPropertyKeyValues(numberOfEdgeProperties, edgePropertyValueLength));
        generateAndWriteEdgeData(person, household, edge, dir, fileName);
    }

    public Vertex createHousehold(Vertex household,
                                  String dir,
                                  String fileName) throws IOException {
        Household h = (Household) household;
        h.setId(i.getAndIncrement());
        h.setLabel(HOUSEHOLD);
        h.setStreet(this.createStreet());
        h.setCity(this.createName(5));
        h.setState(this.createName(2, 0));
        h.setZipcode(this.createNumber(5));
        generateAndWriteVertexData(h, dir, fileName);
        return household;
    }

    public Vertex createAccount(Vertex account,
                                String dir,
                                String fileName) throws IOException {
        Account a = (Account) account;
        a.setId(i.getAndIncrement());
        a.setLabel(ACCOUNT);
        a.setNumber(UUID.randomUUID().toString());
        generateAndWriteVertexData(a, dir, fileName);
        return account;
    }

    public Vertex createPerson(Vertex person,
                               String dir,
                               String fileName) throws IOException {
        Person p = (Person) person;
        p.setId(i.getAndIncrement());
        p.setLabel(PERSON);
        p.setFirstName(this.createName(1, 0).toUpperCase() + this.createName(5));
        p.setLastName(this.createName(1, 0).toUpperCase() + this.createName(10));
        p.setSsn(createNumber(9));
        p.setEmail(createEmail(10));
        p.setPhone(createNumber(10));
        generateAndWriteVertexData(p, dir, fileName);
        return person;
    }

    public Vertex createDevice(Vertex device,
                               String dir,
                               String fileName) throws IOException {
        final Device d = (Device) device;
        d.setId(i.getAndIncrement());
        d.setLabel(DEVICE);
        d.setMacAddress(UUID.randomUUID().toString());
        d.setMake(MAKES.get(this.random.nextInt(MAKES.size() - 1)));
        d.setModel(createName(10));
        generateAndWriteVertexData(d, dir, fileName);
        return device;
    }

    public void generateAndWriteVertexData(Vertex vertex,
                                           String dir,
                                           String fileName) throws IOException {
        final MutablePair<String[], String[]> pair = generateVertexData(vertex);
        generateAndWriteData(pair, dir, fileName);
    }

    public void generateAndWriteEdgeData(Vertex from,
                                         Vertex to,
                                         Edge edge,
                                         String dir,
                                         String fileName) throws IOException {
        final MutablePair<String[], String[]> pair = generateEdgeData(from, to, edge);
        generateAndWriteData(pair, dir, fileName);
    }

    public synchronized void generateAndWriteData(MutablePair<String[], String[]> pair,
                                                  String dir, String fileName) throws IOException {
        int fileCount = 0;
        if (graphMap.containsKey(fileName))
            fileCount = (int) graphMap.get(fileName).get("fileCount");
        Integer countOfRecords = populateGraphMap(dir, fileName, pair, fileCount);
        this.csvWriter.csvwriterWriteToStream((ArrayList<String[]>) this.graphMap.get(fileName).get("data"),
                streamMap.get(dir + "/" + fileName + "_" + fileCount).right);
        HashMap<String, Object> objectPropertyMap = new HashMap<>();
        if (countOfRecords == numberOfRecordsPerFile) {
            if (ENV.equals("aws"))
                this.csvWriter.flushStreamToS3(streamMap.get(dir + "/" + fileName + "_" + fileCount).left.toByteArray(), dir + "/" + fileName + "_" + fileCount);
            else
                this.csvWriter.flushStreamToFile(streamMap.get(dir + "/" + fileName + "_" + fileCount).left.toByteArray(), dir + "/" + fileName + "_" + fileCount);
            streamMap.remove(dir + "/" + fileName + "_" + fileCount);
            objectPropertyMap.put("fileCount", fileCount + 1);
            objectPropertyMap.put("countOfRecords", 0);
            HashSet<String[]> schemaSet = new HashSet<>();
            schemaSet.add(pair.left);
            objectPropertyMap.put("schema", schemaSet);
            //clear the old flushed data from the map to write a new batch
            objectPropertyMap.put("data", new ArrayList<>());
            graphMap.put(fileName, objectPropertyMap);
        }
    }

    public synchronized Integer populateGraphMap(final String dir, final String fileName, final MutablePair<String[], String[]> pair, final int fileCount) throws IOException {
        HashMap<String, Object> objectPropertyMap;
        if (!graphMap.containsKey(fileName)) {
            objectPropertyMap = new HashMap<>();
            //update file fileCount to be appended to the output file
            objectPropertyMap.put("fileCount", fileCount);
            objectPropertyMap.put("countOfRecords", 0);
            HashSet<String[]> schemaSet = new HashSet<>();
            schemaSet.add(pair.left);
            objectPropertyMap.put("schema", schemaSet);
            objectPropertyMap.put("data", new ArrayList<>());
            graphMap.put(fileName, objectPropertyMap);
        } else { 
            objectPropertyMap = graphMap.get(fileName);
        }

        if (!streamMap.containsKey(dir + "/" + fileName + "_" + fileCount)) {
            HashSet<String[]> schemaSet = new HashSet<>();
            schemaSet.add(pair.left);
            this.csvWriter.populateStreamMap(dir + "/" + fileName + "_" + fileCount);
            this.csvWriter.csvwriterWriteToStream(new ArrayList<>(schemaSet), streamMap.get(dir + "/" + fileName + "_" + fileCount).right);
        }

        int countOfRecords = (int) objectPropertyMap.get("countOfRecords");
        ArrayList<String[]> dataList = new ArrayList<>();
        dataList.add(pair.right);
        objectPropertyMap.put("data", dataList);
        objectPropertyMap.put("countOfRecords", countOfRecords + 1);
        graphMap.put(fileName, objectPropertyMap);
        return countOfRecords + 1;
    }

    public synchronized MutablePair<String[], String[]> generateVertexData(Vertex vertex) {
        ArrayList<String> vertexheaders = new ArrayList<>(verticesHeaders);
        vertexheaders.addAll(vertex.keys());
        ArrayList<String> data = new ArrayList<>();
        data.add(vertex.getId().toString());
        data.add(vertex.getLabel());

        for (String key : vertex.keys())
            data.add(vertex.getValueMap().get(key).toString());

        return new MutablePair<>(Arrays.copyOf(vertexheaders.toArray(), vertexheaders.toArray().length, String[].class),
                Arrays.copyOf(data.toArray(), data.toArray().length, String[].class));
    }

    public synchronized MutablePair<String[], String[]> generateEdgeData(Vertex from,
                                                                         Vertex to,
                                                                         Edge edge) {
        ArrayList<String> data = new ArrayList<>();
        data.add(edge.getId().toString());
        data.add(edge.getLabel());
        data.add(from.getId().toString());
        data.add(to.getId().toString());
        data.addAll(edge.getProperties().values());

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
        for (int i = 0; i < length; i++)
            number = number + this.random.nextInt(9);
        return Long.valueOf(number);
    }

    public String createEmail(final int meanLength) {
        return this.createName(meanLength) + "@" + this.createName(5) + DOMAINS.get(this.random.nextInt(DOMAINS.size() - 1));
    }

    public String createStreet() {
        String street = "";
        for (int i = 0; i < this.getGaussian(6, 2); i++)
            street = street + this.random.nextInt(9);
        return street + " " + this.createName(10);
    }

    private final long getGaussian(int mean, int variance) {
        return Math.round(mean + this.random.nextGaussian() * variance);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class CsvWriter {
        private final String path;

        private final HashMap<String, MutablePair<ByteArrayOutputStream, OutputStreamWriter>> streamMap;

        public CsvWriter(String path, HashMap<String, MutablePair<ByteArrayOutputStream, OutputStreamWriter>> map) {
            this.path = path;
            this.streamMap = map;
        }

        public void populateStreamMap(String filePath) throws IOException {
            java.io.File file = new java.io.File(path + "/" + filePath + ".csv");
            file.getParentFile().mkdirs();
            boolean exists = Files.exists(Paths.get(path));
            if (exists) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                OutputStreamWriter writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
                streamMap.put(filePath, new MutablePair<>(stream, writer));
            } else {
                throw new IOException("path creation failed");
            }
        }

        private CSVWriter buildCSVWriter(OutputStreamWriter streamWriter) {
            return new CSVWriter(streamWriter, ',', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        }

        private void csvwriterWriteToStream(List<String[]> list, OutputStreamWriter streamWriter) throws IOException {
            CSVWriter writer = buildCSVWriter(streamWriter);
            writer.writeAll(list);
            writer.flush();
        }

        private void flushStreamToFile(byte[] stream, String filePath) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byteArrayOutputStream.writeBytes(stream);
            try (OutputStream outputStream = new FileOutputStream(path + "/" + filePath + ".csv")) {
                byteArrayOutputStream.writeTo(outputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void flushStreamToS3(byte[] stream, String filePath) {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(stream.length);
            S3_CLIENT.putObject(bucket, path + "/" + filePath + ".csv",
                    new ByteArrayInputStream(stream), meta);
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class Builder {
        protected Logger logger = LoggerFactory.getLogger(IdentityGenerator.class);
        protected int numberOfHouseholds;
        protected int peoplePerHousehold;
        protected int devicesPerPerson;
        protected int accountsPerHousehold;
        protected int numberOfEdgeProperties = 0;
        protected int edgePropertyValueLength;
        protected String id = UUID.randomUUID().toString();
        protected int ops = 10000;
        protected CommandLine cmd;

        public static Builder create() {
            return new Builder();
        }

        public Builder cmdLineArgs(CommandLine cmd) {
            this.cmd = cmd;
            return this;
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

        public Builder numberOfEdgeProperties(final int numberOfEdgeProperties) {
            this.numberOfEdgeProperties = numberOfEdgeProperties;
            return this;
        }

        public Builder edgePropertyValueLength(final int edgePropertyValueLength) {
            this.edgePropertyValueLength = edgePropertyValueLength;
            return this;
        }


        public IdentityGenerator generate() {
            return new IdentityGenerator(this);
        }

        public String toString() {
            return "IdentityGraph.Builder[" +
                    "id:" + this.id +
                    ",numberOfHouseholds:" + this.numberOfHouseholds +
                    ",peoplePerHousehold:" + this.peoplePerHousehold +
                    ",devicesPerHousehold:" + this.devicesPerPerson +
                    ",opsPerTransaction:" + this.ops +
                    ",numberOfEdgeProperties:" + this.numberOfEdgeProperties +
                    ",edgePropertyValueLength:" + this.edgePropertyValueLength +"]";
        }
    }
}
