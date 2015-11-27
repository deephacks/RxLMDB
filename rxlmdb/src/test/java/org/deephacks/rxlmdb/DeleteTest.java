package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.Arrays;
import java.util.LinkedList;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.deephacks.rxlmdb.Fixture.__2;
import static org.deephacks.rxlmdb.Fixture.__3;
import static org.deephacks.rxlmdb.RxObservables.toStreamBlocking;
import static org.junit.Assert.assertTrue;

public class DeleteTest {
  RxDB db;
  RxLMDB lmdb;

  @Before
  public void before() {
    lmdb = RxLMDB.tmp();
    db = lmdb.dbBuilder().build();
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
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
    db.delete(Observable.from(Arrays.asList(__2, __3)));
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

}
