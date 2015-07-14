# RxLMDB
[![Build Status](https://travis-ci.org/deephacks/RxLMDB.svg?branch=master)](https://travis-ci.org/deephacks/RxLMDB)

RxLMDB provide a [RxJava](https://github.com/ReactiveX/RxJava) API to [LMDB](http://symas.com/mdb/) (through [lmdbjni](https://github.com/deephacks/lmdbjni)) which is an ultra-fast, ultra-compact key-value embedded data store developed by Symas for the OpenLDAP Project. LMDB uses memory-mapped files, so it has the read performance of a pure in-memory database while still offering the persistence of standard disk-based databases. Transactional with full ACID semantics and crash-proof by design. No corruption. No startup time. Zero-config cache tuning.

### Why Rx + LMDB?

Java 8 and RxJava is a pleasure to work with but since the LMDB API is a bit low level it make sense to raise the abstraction level to modern standards without scarifying too much (??) performance. So extending LMDB with RxJava makes it possible for asynchronous and event-based programs to process data from LMDB as sequences and adds operators that allow you to compose sequences together declaratively while abstracting away concerns about things like low-level threading, synchronization, thread-safety and concurrent data structures.


### Benchmark

##### Conclusion

* If you want to run slow, copy and parse everything, like protobuf.
* If you want to run fast, zero-copy and parse only what you need.
* If you want to run faster, also use parallel range scans.
* If you want to run fastest, do not use RxLMDB, but plain LMDB.
* Better hardware obviously hardware matters, even more apparent with zero-copy.

##### 3.16.0-4-amd64, Linux Intel(R) Core(TM)2 Quad CPU Q6600 @ 2.40GHz

1 Thread

```bash
Benchmark                            Mode  Cnt        Score         Error  Units
BigKeyValueForwardRangeScan.plain   thrpt   10  1178232.202 ±   81015.649  ops/s
BigKeyValueForwardRangeScan.rx      thrpt   10  1162131.060 ±  112057.128  ops/s
BigZeroCopyForwardRangeScan.plain   thrpt   10  9299859.225 ± 2529503.812  ops/s
KeyValueForwardRangeScan.plain      thrpt   10  6674117.744 ± 1067856.172  ops/s
KeyValueForwardRangeScan.rx         thrpt   10  5323064.014 ± 1061179.864  ops/s
KeyValueForwardSkipRangeScan.plain  thrpt   10  8789483.189 ±  768294.614  ops/s
KeyValueForwardSkipRangeScan.rx     thrpt   10  6453558.501 ±  903252.457  ops/s
ProtoForwardRangeScan.plain         thrpt   10   977556.340 ±  263740.090  ops/s
ProtoForwardRangeScan.rx            thrpt   10   842469.488 ±  170672.957  ops/s
SbeForwardRangeScan.plain           thrpt   10  5924733.706 ± 1985892.580  ops/s
SbeForwardRangeScan.rx              thrpt   10  4570195.110 ±  500547.365  ops/s
ValsForwardRangeScan.plain          thrpt   10  5365088.191 ± 2345685.548  ops/s
ValsForwardRangeScan.rx             thrpt   10  3627839.672 ± 1284540.222  ops/s
```

4 Threads

```bash
Benchmark                            Mode  Cnt         Score         Error  Units
BigKeyValueForwardRangeScan.plain   thrpt   10   1978242.823 ±  174190.990  ops/s
BigKeyValueForwardRangeScan.rx      thrpt   10   1699797.802 ±  147330.769  ops/s
BigZeroCopyForwardRangeScan.plain   thrpt   10  18631395.953 ± 7500005.892  ops/s
KeyValueForwardRangeScan.plain      thrpt   10  13384190.029 ± 2015610.137  ops/s
KeyValueForwardRangeScan.rx         thrpt   10   8646695.332 ± 2026413.388  ops/s
KeyValueForwardSkipRangeScan.plain  thrpt   10  14736089.587 ± 2432557.384  ops/s
KeyValueForwardSkipRangeScan.rx     thrpt   10  12330989.000 ±  559894.869  ops/s
ProtoForwardRangeScan.plain         thrpt   10    651203.480 ±   28715.405  ops/s
ProtoForwardRangeScan.rx            thrpt   10    617451.737 ±   20311.644  ops/s
SbeForwardRangeScan.plain           thrpt   10   8991860.431 ±  465302.254  ops/s
SbeForwardRangeScan.rx              thrpt   10   4755629.167 ± 1821428.568  ops/s
ValsForwardRangeScan.plain          thrpt   10   8546665.500 ± 1269468.808  ops/s
ValsForwardRangeScan.rx             thrpt   10   5812951.172 ±  573829.010  ops/s
```

##### 3.16.0-4-amd64, Intel(R) Core(TM) i7-3740QM CPU @ 2.70GHz

8 threads

```bash
Benchmark                            Mode  Cnt         Score          Error  Units
BigKeyValueForwardRangeScan.plain   thrpt   10  10933343.150 ±    89647.695  ops/s
BigKeyValueForwardRangeScan.rx      thrpt   10   9537755.671 ±    79734.499  ops/s
BigZeroCopyForwardRangeScan.plain   thrpt   10  67753109.315 ± 25043041.656  ops/s
KeyValueForwardRangeScan.plain      thrpt   10  51957281.758 ±  1315119.210  ops/s
KeyValueForwardRangeScan.rx         thrpt   10  37261517.010 ±  2339705.356  ops/s
KeyValueForwardSkipRangeScan.plain  thrpt   10  72329999.694 ±  9638355.993  ops/s
KeyValueForwardSkipRangeScan.rx     thrpt   10  49290830.102 ±  6230559.413  ops/s
ProtoForwardRangeScan.plain         thrpt   10   2043454.082 ±    96951.493  ops/s
ProtoForwardRangeScan.rx            thrpt   10   2129419.080 ±   199508.987  ops/s
SbeForwardRangeScan.plain           thrpt   10  59222391.194 ±  8513022.888  ops/s
SbeForwardRangeScan.rx              thrpt   10  43212267.029 ±  1891687.949  ops/s
ValsForwardRangeScan.plain          thrpt   10  54333422.372 ±  2917837.551  ops/s
ValsForwardRangeScan.rx             thrpt   10  39036264.187 ±  2346692.590  ops/s
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
  Observable<KeyValue> o = db.get(Observable.just(new byte[] { 1 }));

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

