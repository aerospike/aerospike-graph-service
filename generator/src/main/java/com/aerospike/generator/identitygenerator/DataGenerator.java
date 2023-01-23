package com.aerospike.generator.identitygenerator;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.generator.identitygenerator.IdentityGenerator.Builder;
import org.apache.commons.configuration2.Configuration;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.aerospike.generator.util.BulkLoaderConfigHelper.getConfig;

public class DataGenerator {
    private static Configuration CONFIG;
    public static void main(String[] args) {
        System.out.println("Main thread is - " + Thread.currentThread().getName());
        final String defaultConfigPath = "conf/spark-bulk-loader-conf/config.properties";
        final Path path = Path.of(defaultConfigPath);
        CONFIG = getConfig(path);
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            ExecutorService service = Executors.newFixedThreadPool(10);
            Builder builder = Builder.create();
            builder = builder.opsPerTransaction(20).accountsPerHousehold(20).peoplePerHousehold(20).devicesPerPerson(20).households(20);
            IdentityGenerator ig1 = builder.generate(graph);
            Future future = service.submit(ig1);
            ig1.setFuture(future);
            future.get();

//            Thread t1 = new Thread(ig1);
//            t1.start();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}