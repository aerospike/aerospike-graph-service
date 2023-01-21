package com.aerospike.generator.identitygenerator;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.io.*;

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
class IdentityGenerator implements Runnable {

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

    private final Graph graph;
    private final Builder builder;

    private final CsvWriter csvWriter;
    private final Random random = new Random();
    private final Logger LOG;

    private final PrintWriter personVertexWriter;
    private final PrintWriter accountVertexWriter;
    private final PrintWriter householdVertexWriter;
    private final PrintWriter deviceVertexWriter;

    private final PrintWriter holdsEgdeWriter;
    private final PrintWriter partOfEdgeWriter;
    private final PrintWriter ownsEdgeWriter;
    private final PrintWriter subaccountEdgeWriter;

    IdentityGenerator(final Builder builder) {
        this.builder = builder;
        this.graph = builder.graph;
        this.csvWriter = new CsvWriter("/Users/mbelsare/Downloads/datagenerator", builder);
        this.LOG = builder.logger;

        try {
            personVertexWriter = this.csvWriter.getPrintWriter("vertex/Person", "person.csv");
            accountVertexWriter = this.csvWriter.getPrintWriter("vertex/Account", "account.csv");
            householdVertexWriter = this.csvWriter.getPrintWriter("vertex/Household", "household.csv");
            deviceVertexWriter = this.csvWriter.getPrintWriter("vertex/Device", "device.csv");

            holdsEgdeWriter = this.csvWriter.getPrintWriter("edges/Holds", "holds.csv");
            partOfEdgeWriter = this.csvWriter.getPrintWriter("edges/PartOf", "partof.csv");
            ownsEdgeWriter = this.csvWriter.getPrintWriter("edges/Owns", "owns.csv");
            subaccountEdgeWriter = this.csvWriter.getPrintWriter("edges/SubAccount", "subaccount.csv");
        } catch (java.io.FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        LOG.info("Generating subgraph for worker %s: %s", this.builder.id, this.builder);
        for (int i = 0; i < builder.numberOfHouseholds; i++) {
            final Vertex household = this.createHousehold();
            final long numberOfPeople = this.getGaussian(builder.peoplePerHousehold, 2); // +/- 2 from the mean
            final long numberOfAccounts = this.getGaussian(builder.accountsPerHousehold, 1);
            final List<Vertex> accounts = new ArrayList<>();
            for (int j = 0; j < numberOfAccounts; j++) {
                final Vertex account = this.createAccount();
                if (accounts.size() > 0) {
                    final Vertex rootAccount = accounts.get(this.random.nextInt(accounts.size() - 1));
                    this.createSubAccount(rootAccount, account);
                }
                accounts.add(account);
            }
            for (int j = 0; j < numberOfPeople; j++) {
                final Vertex person = this.createPerson();
                if (accounts.size() > 0) this.createHolds(person, accounts.remove(0));
                this.createPartOf(person, household);
                final long numberOfDevices = this.getGaussian(builder.devicesPerPerson, 2);
                for (int k = 0; k < numberOfDevices; k++) {
                    final Vertex device = this.createDevice();
                    this.createOwns(person, device);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static String vertexString(final Vertex vertex) {
        return vertex.label() + IteratorUtils.list(vertex.properties());
    }

    public Edge createSubAccount(final Vertex account, final Vertex subAccount) {
        final Edge edge = this.csvWriter.write_csv(account.addEdge(SUB_ACCOUNT, subAccount), subaccountEdgeWriter);  // properties on edges?
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Edge createHolds(final Vertex person, final Vertex account) {
        final Edge edge = this.csvWriter.write_csv(person.addEdge(HOLDS, account), holdsEgdeWriter);  // properties on edges?
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Edge createOwns(final Vertex person, final Vertex device) {
        final Edge edge = this.csvWriter.write_csv(person.addEdge(OWNS, device), ownsEdgeWriter);  // properties on edges?
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Edge createPartOf(final Vertex person, final Vertex household) {
        final Edge edge = this.csvWriter.write_csv(person.addEdge(PART_OF, household), partOfEdgeWriter);  // properties on edges?
        LOG.debug("Created %s", edge);
        return edge;
    }

    public Vertex createHousehold() {
        final Vertex household = this.csvWriter.write_csv(this.graph.addVertex(
                T.label, HOUSEHOLD,
                STREET, this.createStreet(),
                CITY, this.createName(5),
                STATE, this.createName(2, 0),
                ZIPCODE, this.createNumber(5)), householdVertexWriter);
        LOG.debug("Created %s", vertexString(household));
        return household;
    }

    public Vertex createAccount() {
        final Vertex account = this.csvWriter.write_csv(this.graph.addVertex(
                T.label, ACCOUNT,
                NUMBER, UUID.randomUUID().toString()), accountVertexWriter);
        LOG.debug("Created %s", vertexString(account));
        return account;
    }

    public Vertex createPerson() {
        final Vertex person = this.csvWriter.write_csv(this.graph.addVertex(
                T.label, PERSON,
                FIRST_NAME, this.createName(1, 0).toUpperCase() + this.createName(5),
                LAST_NAME, this.createName(1, 0).toUpperCase() + this.createName(10),
                SSN, createNumber(9),
                EMAIL, createEmail(10),
                PHONE, createNumber(10)), personVertexWriter);
        LOG.debug("Created %s", vertexString(person));
        return person;
    }

    public Vertex createDevice() {
        final Vertex device = this.csvWriter.write_csv(this.graph.addVertex(
                T.label, DEVICE,
                MAC_ADDRESS, UUID.randomUUID().toString(),
                MAKE, MAKES.get(this.random.nextInt(MAKES.size() - 1)),
                MODEL, createName(10)), deviceVertexWriter);
        LOG.debug("Created %s", vertexString(device));
        return device;
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

    public static class CsvWriter {
        private String path;
        private int maxOps;
        private int counter = 0;

        public CsvWriter(String path, final Builder builder) {
            this.path = path;
            this.maxOps = builder.ops;
        }

        public java.io.PrintWriter getPrintWriter(String dir, String fileName) throws java.io.FileNotFoundException {
            java.io.File file = new java.io.File(path + "/" + dir + "/" + fileName);
            file.getParentFile().mkdirs();
            java.io.PrintWriter writer1 = new java.io.PrintWriter(file);
            PrintWriter writer = new PrintWriter(writer1);
            return writer;
        }

        public <T> T write_csv(final T element, final PrintWriter writer) {
//            java.io.File pathFile = new java.io.File(path);
//            pathFile.mkdirs();
//            java.io.File returnFile = new java.io.File(path + fileName);
//            try {
//
//                com.opencsv.CSVWriter writer = new com.opencsv.CSVWriter(new java.io.FileWriter(returnFile));
//                Object t = (Object)element;
//                String s = t.toString();
//                ArrayList<String[]> list = new java.util.ArrayList<>();
//                list.add(new String[]{s});
//                writer.writeAll(list);
//                writer.flush();
//                writer.close();
            writer.println(element.toString());
            return element;
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
        protected int ops = 10;

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
