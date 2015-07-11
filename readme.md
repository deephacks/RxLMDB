# RxLMDB
[![Build Status](https://travis-ci.org/deephacks/RxLMDB.svg?branch=master)](https://travis-ci.org/deephacks/RxLMDB)

RxLMDB provide a [RxJava](https://github.com/ReactiveX/RxJava) API to [LMDB](http://symas.com/mdb/) (through [lmdbjni](https://github.com/deephacks/lmdbjni)) which is an ultra-fast, ultra-compact key-value embedded data store developed by Symas for the OpenLDAP Project. LMDB uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases. Transactional with full ACID semantics and crash-proof by design. No corruption. No startup time. Zero-config cache tuning.

### Why Rx + LMDB?

Java 8 and RxJava is a pleasure to work with but since the LMDB API is a bit low level it make sense to raise the abstraction level to modern standards without scarifying too much (??) performance. So extending LMDB with RxJava makes it possible for asynchronous and event-based programs to process data from LMDB as sequences and adds operators that allow you to compose sequences together declaratively while abstracting away concerns about things like low-level threading, synchronization, thread-safety and concurrent data structures.


### Benchmark

3.16.0-4-amd64, Linux Intel(R) Core(TM)2 Quad CPU Q6600 @ 2.40GHz == dinosaur hardware

1 Thread

```bash
Benchmark                            Mode  Cnt         Score          Error  Units
BigKeyValueForwardRangeScan.plain   thrpt   10   2228290.779 ±    97740.164  ops/s
BigKeyValueForwardRangeScan.rx      thrpt   10   2007602.631 ±   138191.338  ops/s
BigZeroCopyForwardRangeScan.plain   thrpt   10  24039407.723 ± 10324087.658  ops/s
KeyValueForwardRangeScan.plain      thrpt   10  15013271.530 ±  3823436.116  ops/s
KeyValueForwardRangeScan.rx         thrpt   10  13435477.294 ±  1259562.380  ops/s
KeyValueForwardSkipRangeScan.plain  thrpt   10  19197554.155 ±  2595375.027  ops/s
KeyValueForwardSkipRangeScan.rx     thrpt   10  13054462.708 ±  2487268.135  ops/s
ProtoForwardRangeScan.plain         thrpt   10    709373.171 ±    21087.972  ops/s
ProtoForwardRangeScan.rx            thrpt   10    680306.706 ±    41235.523  ops/s
SbeForwardRangeScan.plain           thrpt   10   9432879.769 ±  1401699.377  ops/s
SbeForwardRangeScan.rx              thrpt   10   6393115.548 ±  1382314.020  ops/s
ValsForwardRangeScan.plain          thrpt   10   8433298.977 ±  2416735.095  ops/s
ValsForwardRangeScan.rx             thrpt   10   5526397.270 ±  1477398.306  ops/s
```

4 Threads

```bash
Benchmark                            Mode  Cnt         Score         Error  Units
BigKeyValueForwardRangeScan.plain   thrpt   10   2104314.199 ±   32905.475  ops/s
BigKeyValueForwardRangeScan.rx      thrpt   10   1924742.396 ±  213965.104  ops/s
BigZeroCopyForwardRangeScan.plain   thrpt   10  13904379.947 ± 4509856.319  ops/s
KeyValueForwardRangeScan.plain      thrpt   10  12336763.908 ± 1312262.879  ops/s
KeyValueForwardRangeScan.rx         thrpt   10   8696330.801 ± 1003123.187  ops/s
KeyValueForwardSkipRangeScan.plain  thrpt   10  14891716.757 ± 1433973.266  ops/s
KeyValueForwardSkipRangeScan.rx     thrpt   10  11236080.325 ±  619902.718  ops/s
ProtoForwardRangeScan.plain         thrpt   10    650877.440 ±   23211.104  ops/s
ProtoForwardRangeScan.rx            thrpt   10    612895.675 ±   20446.720  ops/s
ValsForwardRangeScan.plain          thrpt   10   9533923.849 ±  786857.790  ops/s
ValsForwardRangeScan.rx             thrpt   10   6021647.555 ±  614875.489  ops/s
```

### Maven

```xml
<dependency>
  <groupId>org.deephacks.rxlmdb</groupId>
  <artifactId>rxlmdb</artifactId>
  <version>${rxlmdb.version}</version>
</dependency>

<!-- add lmdbjni platform of choice -->

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-linux64</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-osx64</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-win64</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>

<dependency>
  <groupId>org.deephacks.lmdbjni</groupId>
  <artifactId>lmdbjni-android</artifactId>
  <version>${lmdbjni.version}</version>
</dependency>
```

### Usage

```java
  RxLMDB lmdb = RxLMDB.builder()
    .path("/tmp/rxlmdb")
    .size(ByteUnit.GIGA, 1)
    .build();
    
  RxDB db = RxDB.builder()
    .name("test")
    .lmdb(lmdb)
    .build();
  
  KeyValue[] kvs = new KeyValue[] { 
     new KeyValue(new byte[] { 1 }, new byte[] { 1 }),
     new KeyValue(new byte[] { 2 }, new byte[] { 2 }),
     new KeyValue(new byte[] { 3 }, new byte[] { 3 })
  };
  
  // put
  db.put(Observable.from(kvs));
  
  // get
  Observable<KeyValue> o = db.get(new byte[] { 1 });

  // scan forward
  Observable<List<KeyValue<> o = db.scan();

  // scan backward
  Observable<List<KeyValue<> o = db.scan(KeyRange.backward());

  // scan range forward
  Observable<List<KeyValue<> o = db.scan(
    KeyRange.range(new byte[]{ 1 }, new byte[]{ 2 }
  );
  
  // scan range backward
  Observable<List<KeyValue<> o = db.scan(
    KeyRange.range(new byte[]{ 2 }, new byte[]{ 1 }
  );

  // parallel range scans
  Observable<List<KeyValue>> obs = db.scan(
    KeyRange.range(new byte[]{ 1 }, new byte[]{ 1 }),
    KeyRange.range(new byte[]{ 2 }, new byte[]{ 2 }),
    KeyRange.range(new byte[]{ 3 }, new byte[]{ 3 })
  );
  
  // zero copy parallel range scans
  Observable<List<Byte>> obs = db.scan(
    (key, value) -> key.getByte(0),
    KeyRange.range(new byte[]{ 1 }, new byte[]{ 1 }),
    KeyRange.range(new byte[]{ 2 }, new byte[]{ 2 }),
    KeyRange.range(new byte[]{ 3 }, new byte[]{ 3 })
  );
  
  // count rows  
  Integer count = db.scan()
    .flatMap(Observable::from)
    .count().toBlocking().first();

  // delete
  db.delete(Observable.just(new byte[] { 1 }));
  
  // delete range  
  Observable<byte[]> keys = db.scan()
    .flatMap(Observable::from)
    .map(kv -> kv.key);
  db.delete(keys);
  
```

