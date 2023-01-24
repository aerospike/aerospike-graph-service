package com.aerospike.firefly.generator.identitygraphgenerator;

import com.aerospike.firefly.generator.identitygraphgenerator.IdentityGenerator.Builder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataGenerator {
    public static void main(String[] args) {
        System.out.println("Main thread is - " + Thread.currentThread().getName());
        try {
            ExecutorService service = Executors.newFixedThreadPool(400);
            Builder builder = Builder.create();
            builder = builder.opsPerTransaction(5000)
                    .households(500)
                    .accountsPerHousehold(100)
                    .peoplePerHousehold(10)
                    .devicesPerPerson(5);
            IdentityGenerator identityGenerator = builder.generate();
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
