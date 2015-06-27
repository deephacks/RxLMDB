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
    db.put(Observable.from(oneToNine));
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testParallel() {
    LinkedList<KeyValue> expected = Fixture.range(_2, _7);
    Observable<KeyValue> result = db.scan(
      KeyRange.range(_2, _3),
      KeyRange.range(_4, _5),
      KeyRange.range(_6, _7)
    );
    RxObservables.toStreamBlocking(result)
      .map(kv -> kv.key)
      .sorted(new FastKeyComparator())
      .forEach(key -> assertThat(expected.pollFirst().key).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }


  @Test
  public void testParallelDifferentTx() {
    LinkedList<KeyValue> expected = Fixture.range(_2, _5);
    Observable<KeyValue> result = db.scan(KeyRange.range(_2, _3), KeyRange.range(_4, _5));
    RxObservables.toStreamBlocking(result)
      .map(kv -> kv.key)
      .sorted(new FastKeyComparator())
      .forEach(key -> assertThat(expected.pollFirst().key).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testParallelSameTx() {
    LinkedList<KeyValue> expected = Fixture.range(_2, _5);
    RxTx tx = db.lmdb.readTx();
    Observable<KeyValue> result = db.scan(tx, KeyRange.range(_2, _3), KeyRange.range(_4, _5));
    RxObservables.toStreamBlocking(result)
      .map(kv -> kv.key)
      .sorted(new FastKeyComparator())
      .forEach(key -> assertThat(expected.pollFirst().key).isEqualTo(key));
    assertTrue(expected.isEmpty());
  }
}
