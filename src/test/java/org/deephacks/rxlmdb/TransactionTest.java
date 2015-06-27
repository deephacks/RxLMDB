package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import static org.deephacks.rxlmdb.Fixture.*;

import static com.google.common.truth.Truth.assertThat;
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
    db.put(tx, Observable.from(oneToNine));
    tx.abort();
    db.scan(KeyRange.forward()).toBlocking().first();
  }

  @Test
  public void testCommit() {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(oneToNine));
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(_1, _9);
    db.scan(KeyRange.forward()).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testWriteAndCommitTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    db.put(tx, Observable.from(oneToNine).subscribeOn(Schedulers.io()));
    Thread.sleep(200);
    tx.commit();
    LinkedList<KeyValue> expected = Fixture.range(_1, _9);
    db.scan(KeyRange.forward()).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testCreateAndCommitTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    Observable<KeyValue> obs = Observable.from(oneToNine)
      .subscribeOn(Schedulers.io())
      .finallyDo(() -> tx.commit());
    db.put(tx, obs);
    Thread.sleep(200);
    LinkedList<KeyValue> expected = Fixture.range(_1, _9);
    db.scan(KeyRange.forward()).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test(expected = NoSuchElementException.class)
  public void testCreateAndRollbackTxOnSeparateThreads() throws InterruptedException {
    RxTx tx = lmdb.writeTx();
    Observable<KeyValue> obs = Observable.from(oneToNine)
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
    db.put(tx, Observable.from(oneToNine));
    LinkedList<KeyValue> expected = Fixture.range(_1, _9);
    // should see values within same yet-to-commit tx
    db.scan(tx, KeyRange.forward()).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
    tx.abort();
    // aborted so NoSuchElementException is expected
    db.scan(KeyRange.forward()).toBlocking().first();
  }
}
