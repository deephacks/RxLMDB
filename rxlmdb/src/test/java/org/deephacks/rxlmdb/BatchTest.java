package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BatchTest {
  RxDb db;
  RxLmdb lmdb;

  @Before
  public void before() {
    lmdb = RxLmdb.tmp();
    db = lmdb.dbBuilder().build();
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testBatch() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger();
    db.batch(burst(counter).buffer(100, TimeUnit.MILLISECONDS, 10));
    Thread.sleep(1000);
    List<KeyValue> list = db.scan(KeyRange.forward()).toBlocking().first();
    assertThat(list.size(), is(counter.get()));
    assertThat(list.get(0).key()[0], is((byte) 1));
    assertThat(list.get(list.size() - 1).key()[0], is((byte) counter.get()));
  }

  /**
   * An error item should not affect writing other items.
   */
  @Test
  public void testBatchSingleError() throws InterruptedException {
    PublishSubject<KeyValue> subject = PublishSubject.create();
    db.batch(subject.observeOn(Schedulers.newThread()).buffer(100, TimeUnit.MILLISECONDS, 10));
    subject.onNext(Fixture.values[0]);
    subject.onNext(null);
    subject.onNext(Fixture.values[2]);
    subject.onCompleted();
    Thread.sleep(500);
    List<KeyValue> list = db.scan(KeyRange.forward()).toBlocking().first();
    assertThat(list.size(), is(2));
  }

  static Observable<KeyValue> burst(AtomicInteger counter) {
    return Observable.create((Subscriber<? super KeyValue> s) -> {
      int rounds = 5;
      while (!s.isUnsubscribed()) {
        // burst some number of items
        int num = (int) (Math.random() * 10);
        for (int i = 0; i < num; i++) {
          int j = counter.incrementAndGet();
          s.onNext(new KeyValue(new byte[] { (byte) j }, new byte[] {(byte) j}));
        }
        try {
          // sleep for a random amount of time
          Thread.sleep((long) (Math.random() * 100));
        } catch (Exception e) {
          // do nothing
        }
        if (rounds-- == 0) {
          s.onCompleted();
          return;
        }
      }
    }).subscribeOn(Schedulers.newThread());
  }
}
