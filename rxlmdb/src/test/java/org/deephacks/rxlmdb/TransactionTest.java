package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import static org.deephacks.rxlmdb.Fixture.*;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.RxObservables.toStreamBlocking;
import static org.junit.Assert.assertTrue;

public class TransactionTest {
  RxDB db;
  RxLMDB lmdb;

  @Before
  public void before() {
    lmdb = RxLMDB.tmp();
    db = RxDB.builder().lmdb(lmdb).build();
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test(expected = NoSuchElementException.class)
  public void testAbort() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.abort();
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test
  public void testAbortDelete() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.commit();
    tx = lmdb.writeTx();
    db.delete(tx, Observable.from(keys));
    assertThat(toStreamBlocking(db.scan(tx, KeyRange.forward())).count()).isEqualTo(0L);
    tx.abort();
    assertThat(toStreamBlocking(db.scan(KeyRange.forward())).count()).isEqualTo(9L);
  }

  @Test
  public void testCommit() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testCommitDelete() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.commit();
    tx = lmdb.writeTx();
    db.delete(tx, Observable.from(keys));
    assertThat(toStreamBlocking(db.scan(tx, KeyRange.forward())).count()).isEqualTo(0L);
    tx.commit();
    assertThat(toStreamBlocking(db.scan(KeyRange.forward())).count()).isEqualTo(0L);
  }

  @Test
  public void testDeleteAll() {
    db.put(Observable.from(_1_to_9));
    db.delete();
    assertThat(toStreamBlocking(db.scan(KeyRange.forward())).count()).isEqualTo(0L);
  }

  @Test
  public void testDeleteAllAbort() {
    db.put(Observable.from(_1_to_9));
    RxTx tx = lmdb.writeTx();
    db.delete(tx);
    tx.abort();
    assertThat(toStreamBlocking(db.scan(KeyRange.forward())).count()).isEqualTo(9L);
  }

  @Test
  public void testDeleteKeys() throws InterruptedException {
    db.put(Observable.from(_1_to_9));
    db.delete(Observable.from(Arrays.asList( __2, __3)));
    assertThat(toStreamBlocking(db.scan(KeyRange.forward())).count()).isEqualTo(7L);
  }

  @Test
  public void testDeleteKeysAbort() {
    db.put(Observable.from(_1_to_9));
    RxTx tx = lmdb.writeTx();
    db.delete(tx, Observable.from(Arrays.asList(__2, __3)));
    tx.abort();
    assertThat(toStreamBlocking(db.scan(KeyRange.forward())).count()).isEqualTo(9L);
  }


  @Test
  public void testDeleteRange() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    tx.commit();
    tx = lmdb.writeTx();
    Observable<byte[]> keys = db.scan(tx, KeyRange.atMost(new byte[]{5, 5}))
      .flatMap(Observable::from)
      .map(kv -> kv.key);
    db.delete(tx, keys);
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(__6, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testWriteAndCommitTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9).subscribeOn(Schedulers.io()));
    Thread.sleep(200);
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testCreateAndCommitTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    Observable<KeyValue> obs = Observable.from(_1_to_9)
      .subscribeOn(Schedulers.io())
      .finallyDo(() -> tx.commit());
    db.put(tx, obs);
    Thread.sleep(200);
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test(expected = NoSuchElementException.class)
  public void testCreateAndRollbackTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    Observable<KeyValue> obs = Observable.from(_1_to_9)
      .subscribeOn(Schedulers.io())
      .finallyDo(() -> tx.abort());
    db.put(tx, obs);
    Thread.sleep(200);
    // aborted so NoSuchElementException is expected
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test(expected = NoSuchElementException.class)
  public void testScanWithinTxThenAbort() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(_1_to_9));
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    // should see values within same yet-to-commit tx
    toStreamBlocking(db.scan(tx, KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
    tx.abort();
    // aborted so NoSuchElementException is expected
    db.scan(KeyRange.forward()).toBlocking().first();
  }
}
