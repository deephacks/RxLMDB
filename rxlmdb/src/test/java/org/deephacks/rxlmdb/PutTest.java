package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.deephacks.rxlmdb.Fixture.__3;
import static org.deephacks.rxlmdb.Fixture.values;
import static org.deephacks.rxlmdb.RxObservables.toStreamBlocking;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class PutTest {
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

  @Test(expected = NoSuchElementException.class)
  public void testPutAbort() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.abort();
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test(expected = NoSuchElementException.class)
  public void testPutAbortWithResources() {
    try (RxTx tx = lmdb.writeTx()) {
      db.put(tx, Observable.from(_1_to_9));
    }
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test
  public void testPut() {
    db.put(Fixture.values[0]);
    assertArrayEquals(db.get(__1), __1);
  }

  @Test
  public void testPutCommit() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testAppendCommit() {
    RxTx tx = lmdb.writeTx();
    db.append(tx, Observable.from(_1_to_9));
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertTrue(expected.isEmpty());
  }

  @Test(expected = NoSuchElementException.class)
  public void testPutException() throws InterruptedException {
    AtomicReference<Throwable> t = new AtomicReference<>();
    db.put(Observable.just((KeyValue) null)
      .doOnError(throwable -> t.set(throwable)));
    assertThat(t.get()).isInstanceOf(NullPointerException.class);
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test
  public void testPutOnErrorResumeNext() {
    AtomicReference<Throwable> t = new AtomicReference<>();
    db.put(Observable.from(new KeyValue[]{values[0], null, values[2]})
      .onErrorResumeNext(throwable -> {
        t.set(throwable);
        return Observable.just(values[1]);
      }));
    LinkedList<KeyValue> expected = Fixture.range(__1, __3);
    toStreamBlocking(db.scan())
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(t.get()).isInstanceOf(NullPointerException.class);
  }

  @Test(expected = NoSuchElementException.class)
  public void testPutDoOnErrorAbort() throws InterruptedException {
    AtomicReference<Throwable> t = new AtomicReference<>();
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(new KeyValue[]{values[0], null, values[2]})
      .doOnError(throwable -> {
        t.set(throwable);
        tx.abort();
      }));
    assertThat(t.get()).isInstanceOf(NullPointerException.class);
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test
  public void testPutExceptionAsync() throws InterruptedException {
    AtomicReference<Throwable> t = new AtomicReference<>();
    db.put(Observable.just((KeyValue) null)
      .observeOn(Schedulers.io())
      .doOnError(throwable -> t.set(throwable)));
    Thread.sleep(100);
    LinkedList<KeyValue> expected = Fixture.range(__1, __3);
    toStreamBlocking(db.scan())
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(t.get()).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testPutAndCommitTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    Observable<KeyValue> obs = Observable.from(_1_to_9)
      .subscribeOn(Schedulers.io());
    db.put(tx, obs);
    Thread.sleep(200);
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test(expected = NoSuchElementException.class)
  public void testPutAndRollbackTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    Observable<KeyValue> obs = Observable.from(_1_to_9)
      .subscribeOn(Schedulers.io());
    db.put(tx, obs);
    Thread.sleep(200);
    tx.abort();
    // aborted so NoSuchElementException is expected
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test(expected = NoSuchElementException.class)
  public void testPutScanWithinTxThenAbort() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    // should see values within same yet-to-commit tx
    toStreamBlocking(db.scan(tx, KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
    tx.abort();
    // aborted so NoSuchElementException is expected
    db.scan(KeyRange.forward()).toBlocking().first();
  }
}
