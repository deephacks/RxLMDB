java -jar target/benchmarks.jar org.deephacks.rxlmdb.JMH.rxMulti -jvmArgs="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=duration=60s,filename=rxmulti.jfr"
