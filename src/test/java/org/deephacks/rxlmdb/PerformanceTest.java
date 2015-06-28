package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;

import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PerformanceTest {
  RxDB db;
  RxLMDB lmdb;

  @Before
  public void before() {
    lmdb = RxLMDB.builder().size(ByteUnit.GIGA, 1).build();
    db = RxDB.builder().lmdb(lmdb).build();
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Ignore
  public void perfTest() {
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
          System.out.println(j);
        }
        subscriber.onCompleted();
        System.out.println("commit done");
      }
    }).doOnError(throwable -> throwable.printStackTrace());
    db.put(observable);

    System.out.println("single");
    for (int i = 1; i < 10; i++) {
      long before = System.currentTimeMillis();
      Observable<List<KeyValue>> obs = db.scan();
      AtomicInteger counter = new AtomicInteger();
      obs.toBlocking().forEach(kvs -> kvs.stream().forEach(kv -> counter.incrementAndGet()));
      System.out.println(counter.get() + " items took " + (System.currentTimeMillis() - before) + "ms");
    }
    System.out.println("multi ");
    for (int i = 1; i < 10; i++) {
      long before = System.currentTimeMillis();
      Observable<List<KeyValue>> obs = db.scan(
        KeyRange.range(new byte[]{0}, new byte[]{0}),
        KeyRange.range(new byte[]{1}, new byte[]{1}),
        KeyRange.range(new byte[]{2}, new byte[]{2}),
        KeyRange.range(new byte[]{3}, new byte[]{3}));
      AtomicInteger counter = new AtomicInteger();
      obs.toBlocking().forEach(kvs -> kvs.stream().forEach(kv -> counter.incrementAndGet()));
      System.out.println(counter.get() + " items took " + (System.currentTimeMillis() - before) + "ms");
    }
  }
}
