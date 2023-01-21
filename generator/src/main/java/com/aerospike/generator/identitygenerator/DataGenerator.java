package com.aerospike.generator.identitygenerator;

import com.aerospike.generator.identitygenerator.IdentityGenerator.*;

public class DataGenerator {
    public static void main(String[] args) {
        System.out.println("Main thread is - " + Thread.currentThread().getName());
        Builder builder = Builder.create();
//        IdentityGenerator ig = builder.generate()
        builder = builder.opsPerTransaction(10).accountsPerHousehold(10).peoplePerHousehold(4).devicesPerPerson(3).households(10);
        Thread t1 = new Thread(new IdentityGenerator(builder));
        t1.start();
    }
}