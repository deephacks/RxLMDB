package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Transaction;
import org.openjdk.jmh.annotations.*;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class JMH {
  RxDB db;
  RxLMDB lmdb;

  @Setup()
  public void setup() throws IOException {
    Files.createDirectories(Paths.get("/tmp/rxlmdb-jmh"));
    lmdb = RxLMDB.builder().path("/tmp/rxlmdb-jmh").size(ByteUnit.GIGA, 1).build();
    db = RxDB.builder().lmdb(lmdb).build();
    if (db.scan().count().toBlocking().first() != 0) {
      return;
    }
    Observable<KeyValue> observable = Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        for (int j = 0; j < 4; j++) {
          DirectBuffer key = new DirectBuffer(new byte[5]);
          for (int i = 0; i < 500_000; i++) {
            key.putByte(0, (byte) j);
            key.putInt(1, i, ByteOrder.BIG_ENDIAN);
            subscriber.onNext(new KeyValue(key.byteArray(), key.byteArray()));
          }
        }
        subscriber.onCompleted();
      }
    }).doOnError(throwable -> throwable.printStackTrace());
    db.put(observable);
  }

  Iterator<List<KeyValue>> obs;
  Iterator<KeyValue> values;

  @Benchmark
  public void rx() {
    if (obs == null) {
      obs = db.scan().toBlocking().toIterable().iterator();
      values = obs.next().iterator();
    }
    if (values.hasNext()) {
      values.next();
    } else if (obs.hasNext()) {
      values = obs.next().iterator();
    } else {
      obs = db.scan().toBlocking().toIterable().iterator();
      values = obs.next().iterator();
    }
  }

  BufferCursor cursor;
  Transaction tx;
  
  @Benchmark
  public void plain() {
    if (cursor == null) {
      tx = lmdb.env.createReadTransaction();
      cursor = db.db.bufferCursor(tx);
    }
    if (cursor.next()) {
      new KeyValue(cursor.keyBytes(), cursor.valBytes());
    } else {
      cursor.first();
    }
  }
}
