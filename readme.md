# RxLMDB
[![Build Status](https://travis-ci.org/deephacks/RxLMDB.svg?branch=master)](https://travis-ci.org/deephacks/RxLMDB)

RxLMDB provide a [RxJava](https://github.com/ReactiveX/RxJava) API to [LMDB](http://symas.com/mdb/) (through [lmdbjni](https://github.com/deephacks/lmdbjni)) which is an ultra-fast, ultra-compact key-value embedded data store developed by Symas for the OpenLDAP Project. LMDB uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases. Transactional with full ACID semantics and crash-proof by design. No corruption. No startup time. No dependencies.

### Why Rx + LMDB?

Java 8 and RxJava is a pleasure to work with but since the LMDB API is a bit low level it make sense to raise the abstraction level to modern standards without scarifying too much (??) performance. So extending LMDB with RxJava makes it possible for asynchronous and event-based programs to process data from LMDB as sequences and adds operators that allow you to compose sequences together declaratively while abstracting away concerns about things like low-level threading, synchronization, thread-safety and concurrent data structures.


### Benchmark

1 Thread

```bash
Benchmark                           Mode  Cnt        Score         Error  Units
KeyValueForwardRangeScan.plain     thrpt   10  5855981.363 ± 1842051.132  ops/s
KeyValueForwardRangeScan.rx        thrpt   10   934573.973 ±  310719.229  ops/s
KeyValueForwardRangeScanBig.plain  thrpt   10   981244.738 ±  185041.091  ops/s
KeyValueForwardRangeScanBig.rx     thrpt   10   117232.300 ±  101469.746  ops/s
ProtoForwardRangeScan.plain        thrpt   10   719921.586 ±  350020.287  ops/s
ProtoForwardRangeScan.rx           thrpt   10   324075.789 ±  137832.590  ops/s
ValsForwardRangeScan.plain         thrpt   10  7967767.285 ± 2684515.075  ops/s
ValsForwardRangeScan.rx            thrpt   10   472333.649 ±  263407.110  ops/s
ZeroCopyForwardRangeScanBig.plain  thrpt   10  6608123.744 ± 2399760.309  ops/s
```

4 Threads

```bash
Benchmark                           Mode  Cnt         Score         Error  Units
KeyValueForwardRangeScan.plain     thrpt   10  14510002.689 ± 1265992.980  ops/s
KeyValueForwardRangeScan.rx        thrpt   10   1960971.506 ±  414484.221  ops/s
KeyValueForwardRangeScanBig.plain  thrpt   10   1729805.004 ±  202788.578  ops/s
KeyValueForwardRangeScanBig.rx     thrpt   10    390879.683 ±   38877.376  ops/s
ProtoForwardRangeScan.plain        thrpt   10    678630.866 ±   85316.353  ops/s
ProtoForwardRangeScan.rx           thrpt   10    617289.898 ±   50765.237  ops/s
ValsForwardRangeScan.plain         thrpt   10  14441259.934 ± 3606977.540  ops/s
ValsForwardRangeScan.rx            thrpt   10   1140714.164 ±  190646.443  ops/s
ZeroCopyForwardRangeScanBig.plain  thrpt   10  10137192.818 ± 6241610.057  ops/s
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

