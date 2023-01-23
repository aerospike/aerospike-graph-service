package com.aerospike.generator.identitygenerator;

import com.aerospike.generator.identitygenerator.IdentityGenerator.Builder;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataGenerator {
    public static void main(String[] args) {
        System.out.println("Main thread is - " + Thread.currentThread().getName());
        try (final Graph graph = TinkerGraph.open()) {
            ExecutorService service = Executors.newFixedThreadPool(200);
            Builder builder = Builder.create();
            builder = builder.opsPerTransaction(1000)
                    .households(200)
                    .accountsPerHousehold(100)
                    .peoplePerHousehold(10)
                    .devicesPerPerson(5);
            IdentityGenerator identityGenerator = builder.generate(graph);
            Future future = service.submit(identityGenerator);
            identityGenerator.setFuture(future);
            future.get();
            if ( future.isDone() )
                service.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}