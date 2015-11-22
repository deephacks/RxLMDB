package org.deephacks.rxlmdb;

import org.openjdk.jmh.annotations.*;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 5)
@Fork(value = 1)
@Threads(value = 12)
public class PutTest {

  static RxDB db;
  static RxLMDB lmdb;
  static SerializedSubject<KeyValue, KeyValue> subject;

  static {
    try {
      Path path = Paths.get("/tmp/rxlmdb-jmh-BatchTest");
      Files.createDirectories(path);
      lmdb = RxLMDB.builder().path(path).size(ByteUnit.GIGA, 1).build();
      db = RxDB.builder().lmdb(lmdb).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    subject = PublishSubject.<KeyValue>create().toSerialized();
    db.batch(subject.buffer(10, TimeUnit.NANOSECONDS, 4096));
  }

  @State(Scope.Thread)
  public static class RxThread {
    public static final byte[] _1 = new byte[]{1};
    public static final byte[] _2 = new byte[]{2};
    public static final byte[] _3 = new byte[]{3};
    public static final byte[] _4 = new byte[]{4};
    public static final byte[] _5 = new byte[]{5};
    public static final byte[] _6 = new byte[]{6};
    public static final byte[] _7 = new byte[]{7};
    public static final byte[] _8 = new byte[]{8};
    public static final byte[] _9 = new byte[]{9};

    public static final KeyValue[] values = new KeyValue[]{
      new KeyValue(_1, _1),
      new KeyValue(_2, _2),
      new KeyValue(_3, _3),
      new KeyValue(_4, _4),
      new KeyValue(_5, _5),
      new KeyValue(_6, _6),
      new KeyValue(_7, _7),
      new KeyValue(_8, _8),
      new KeyValue(_9, _9)
    };

    public static final Observable[] observables = new Observable[]{
      Observable.just(values[0]),
      Observable.just(values[1]),
      Observable.just(values[2]),
      Observable.just(values[3]),
      Observable.just(values[4]),
      Observable.just(values[5]),
      Observable.just(values[6]),
      Observable.just(values[7]),
      Observable.just(values[8])
    };

    RxTx tx = null;
    AtomicInteger counter = new AtomicInteger();

    public RxThread() {
    }

    public void batch() {
      int i = counter.incrementAndGet();
      subject.onNext(values[i % 9]);
    }

    public void put() {
      if (tx == null) {
        tx = lmdb.writeTx();
      }
      int num = counter.incrementAndGet() % 9;
      db.put(tx, observables[num]);
      if (num == 0) {
        tx.commit();
        tx = null;
      }
    }
  }

  @Benchmark
  public void batch(RxThread t) {
    t.batch();
  }

  @Benchmark
  public void put(RxThread t) {
    t.put();
  }

}
