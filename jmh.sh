#!/bin/bash
mvn clean install -P jmh && java -cp target/rxlmdb-0.0.2-SNAPSHOT-tests.jar:target/jars/jmh-core-1.10.jar:target/benchmarks.jar:target/jars/jopt-simple-4.6.jar:target/jars/commons-math3-3.2.jar org.openjdk.jmh.Main
