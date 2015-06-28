package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.LinkedList;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.junit.Assert.assertTrue;

public class ParallelRangeScanTest {
  RxDB db;

  @Before
  public void before() {
    db = RxDB.tmp();
    db.put(Observable.from(_1_to_9));
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testParallel() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __7);
    Observable<KeyValue> result = db.scan(
      KeyRange.range(__2, __3),
      KeyRange.range(__4, __5),
      KeyRange.range(__6, __7)
    );
    RxObservables.toStreamBlocking(result)
      .map(kv -> kv.key)
      .sorted(DirectBufferComparator.byteArrayComparator())
      .forEach(key -> assertThat(expected.pollFirst().key).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }


  @Test
  public void testParallelDifferentTx() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __5);
    Observable<KeyValue> result = db.scan(KeyRange.range(__2, __3), KeyRange.range(__4, __5));
    RxObservables.toStreamBlocking(result)
      .map(kv -> kv.key)
      .sorted(DirectBufferComparator.byteArrayComparator())
      .forEach(key -> assertThat(expected.pollFirst().key).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testParallelSameTx() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __5);
    RxTx tx = db.lmdb.readTx();
    Observable<KeyValue> result = db.scan(tx, KeyRange.range(__2, __3), KeyRange.range(__4, __5));
    RxObservables.toStreamBlocking(result)
      .map(kv -> kv.key)
      .sorted(DirectBufferComparator.byteArrayComparator())
      .forEach(key -> assertThat(expected.pollFirst().key).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testParallelScanMapper() {
    Scan<Byte> scan = (key, value) -> key.getByte(0);
    LinkedList<KeyValue> expected = Fixture.range(__2, __5);
    RxTx tx = db.lmdb.readTx();
    Observable<Byte> result = db.scan(tx, scan, KeyRange.range(__2, __3), KeyRange.range(__4, __5));

    RxObservables.toStreamBlocking(result).sorted()
      .forEach(key -> assertThat(expected.pollFirst().key[0]).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }
}
