package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyIterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

public class Movielens {
    public static final long vertexCount = 9941;
    public static final long edgeCount = 1006617;
    public static final String GENRE = "genre";
    public static final String YEAR = "year";
    private static final String NAME = "name";
    public static final String MOVIE = "movie";

    private static final Logger LOG = LoggerFactory.getLogger(Movielens.class);
    public static final String MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-1m.zip";
    private static final Map<Integer, Long> movieIdCache = new HashMap<>();
    private static final Map<Integer, Long> userIdCache = new HashMap<>();

    public static interface MovielensElement {
    }

    private static void periodicLog(String name, long metric, AtomicLong checkpoint) {
        final long INCREMENT = 1000;
        final long SECOND = 1000;
        if (metric % INCREMENT == 0) {
            final long now = System.currentTimeMillis();
            final long msPerIncrement = now - checkpoint.get();
            checkpoint.set(now);
            final double msPerElement = msPerIncrement / (INCREMENT + 0.0d);
            final double elePerSec = SECOND / msPerElement;
            LOG.info("loaded {} {} at {}/sec", metric, name, Double.valueOf(elePerSec).longValue());
        }
    }

    public static class Movie implements MovielensElement {
        private static final List<String> format = List.of("MovieID::Title::Genres".split("::"));
        private static final String MOVIE_ID = "movieId";

        public final int movieId;
        public final String movieTitle;
        public final List<String> genres;

        private Movie(int movieId, String movieTitle, List<String> genres) {
            this.movieId = movieId;
            this.movieTitle = movieTitle;
            this.genres = genres;
        }

        static List<String> parseGenres(final String genres) {
            return Arrays.asList(genres.split("\\|"));
        }

        public static Movie fromLine(final String line) {
            final String[] components = line.split("::");
            return new Movie(
                    Integer.parseInt(components[format.indexOf("MovieID")]),
                    components[format.indexOf("Title")],
                    parseGenres(components[format.indexOf("Genres")]));
        }

        public static Iterator<Movie> iterator(final Path moviesDat) {
            try {
                return Files.lines(moviesDat, Charset.forName("Cp1252")).map(line -> Movie.fromLine(line)).iterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Iterator<Vertex> loadVertices(Graph graph, AtomicLong metric, AtomicLong timer) {
            List<Vertex> results = new ArrayList<>();

            ArrayList<Object> properties = new ArrayList<Object>() {
                {
                    add(MOVIE_ID);
                    add(movieId);
                    add(NAME);
                    add(movieTitle);
                    add(T.label);
                    add(MOVIE);
                }
            };
            int movieYear = -1;
            if (movieTitle.strip().endsWith(")")) {
                try {
                    String[] tokens = movieTitle.split("[()]");
                    movieYear = Integer.parseInt(tokens[tokens.length - 1]);
                } catch (Exception e) {
                    movieYear = -1;
                }
            }
            properties.add(YEAR);
            properties.add(movieYear);
            //@todo hack, swap key and value, this should be reversed when multi-properties are supported
            // g.V().has("action","genre")
            genres.forEach(genre -> {
                properties.add(genre);
                properties.add(GENRE);
            });

            Vertex mv = graph.addVertex(properties.toArray());
            movieIdCache.put(movieId, (Long) mv.id());
            results.add(mv);
            metric.incrementAndGet();
            periodicLog(MOVIE, metric.get(), timer);

            return results.iterator();
        }

        public Iterator<Edge> loadEdges(Graph graph, AtomicLong metric, AtomicLong timer) {
            return emptyIterator();
        }

    }

    public static class User implements MovielensElement {
        private static final List<String> format = List.of("UserID::Gender::Age::Occupation::Zip-code".split("::"));
        private static final String USER_ID = "userId";
        private static final String AGE = "age";
        private static final String PERSON = "person";
        public final int userId;
        public final boolean gender;
        public final int age;
        public final String occupation;
        public final String zipcode;
        public final String GENDER = "gender";
        public final String OCCUPATION = "occupation";

        private User(int userId, boolean gender, int age, String occupation, String zipcode) {
            this.userId = userId;
            this.gender = gender;
            this.age = age;
            this.occupation = occupation;
            this.zipcode = zipcode;
        }

        public static User fromLine(final String line) {
            final String[] components = line.split("::");
            return new User(Integer.parseInt(
                    components[format.indexOf("UserID")]),
                    components[format.indexOf("Gender")].equals("M"),
                    Integer.parseInt(components[format.indexOf("Age")]),
                    components[format.indexOf("Occupation")],
                    components[format.indexOf("Zip-code")]);
        }

        public static Iterator<User> iterator(final Path usersDat) {
            try {
                return Files.lines(usersDat).map(line -> User.fromLine(line)).iterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public Iterator<Vertex> loadVertices(final Graph graph, AtomicLong metric, AtomicLong timer) {
            ArrayList<Vertex> results = new ArrayList<Vertex>();
            Vertex uv = graph.addVertex(T.label, PERSON,
                    USER_ID, this.userId,
                    GENDER, this.gender,
                    AGE, this.age,
                    OCCUPATION, this.occupation);

            results.add(uv);
            userIdCache.put(userId, (Long) uv.id());
            metric.addAndGet(results.size());
            periodicLog("user", metric.get(), timer);
            return results.iterator();
        }

        public Iterator<Edge> loadEdges(final Graph graph, AtomicLong metric, AtomicLong timer) {
            return emptyIterator();
        }
    }

    public static class Rating implements MovielensElement {
        private static final List<String> format = List.of("UserID::MovieID::Rating::Timestamp".split("::"));
        private static final String TIME = "time";
        public static final String RATED = "rated";
        public static final String STARS = "stars";
        public final int userId;
        public final int movieId;
        public final int rating;
        public final long timestamp;

        private Rating(int userId, int movieId, int rating, long timestamp) {
            this.userId = userId;
            this.movieId = movieId;
            this.rating = rating;
            this.timestamp = timestamp;
        }

        public static Rating fromLine(final String line) {
            final String[] components = line.split("::");
            return new Rating(
                    Integer.parseInt(components[format.indexOf("UserID")]),
                    Integer.parseInt(components[format.indexOf("MovieID")]),
                    Integer.parseInt(components[format.indexOf("Rating")]),
                    Long.parseLong(components[format.indexOf("Timestamp")]));
        }

        public static Iterator<Rating> iterator(final Path ratingsDat) {
            try {
                return Files.lines(ratingsDat).map(line -> Rating.fromLine(line)).iterator();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void loadVertices(Graph graph, AtomicLong metric, AtomicLong timer) {

        }

        public Iterator<Edge> loadEdges(Graph graph, AtomicLong metric, AtomicLong timer) {
            GraphTraversalSource g = graph.traversal();
            Edge e = g.V(userIdCache.get(userId)).next()
                    .addEdge(RATED,
                            g.V(movieIdCache.get(movieId)).next(),
                            STARS, this.rating, TIME, this.timestamp);
            metric.incrementAndGet();
            periodicLog("rating edges", metric.get(), timer);
            return IteratorUtils.of(e);
        }
    }

    public static void parse(final Path basePath, final Graph graph) {
        AtomicLong m1 = new AtomicLong();
        AtomicLong timer1 = new AtomicLong(System.currentTimeMillis());
        Movie.iterator(basePath.resolve("movies.dat")).forEachRemaining(movie -> movie.loadVertices(graph, m1, timer1));
        LOG.info("Movie count: {}", m1.get());
        AtomicLong m2 = new AtomicLong();
        AtomicLong timer2 = new AtomicLong(System.currentTimeMillis());
        User.iterator(basePath.resolve("users.dat")).forEachRemaining(user -> user.loadVertices(graph, m2, timer2));
        LOG.info("User count: {}", m2.get());
        AtomicLong m4 = new AtomicLong();
        AtomicLong timer4 = new AtomicLong(System.currentTimeMillis());
        Movie.iterator(basePath.resolve("movies.dat")).forEachRemaining(movie -> movie.loadEdges(graph, m4, timer4));
        LOG.info("Movie edges processed: {}", m4.get());
        AtomicLong m5 = new AtomicLong();
        AtomicLong timer5 = new AtomicLong(System.currentTimeMillis());
        User.iterator(basePath.resolve("users.dat")).forEachRemaining(user -> user.loadEdges(graph, m5, timer5));
        LOG.info("User edges processed: {}", m5.get());
        AtomicLong m6 = new AtomicLong();
        AtomicLong timer6 = new AtomicLong(System.currentTimeMillis());
        Rating.iterator(basePath.resolve("ratings.dat")).forEachRemaining(rating -> rating.loadEdges(graph, m6, timer6));
        LOG.info("Ratings edges processed: {}", m6.get());
    }
}
