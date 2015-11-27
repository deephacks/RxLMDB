package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.deephacks.rxlmdb.RxObservables.toStreamBlocking;

public class ScanTest {
  RxDb db;

  @Before
  public void before() {
    db = RxDb.tmp();
    db.put(Observable.from(_1_to_9));
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testScanRangeForward() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __3);
    toStreamBlocking(db.scan(KeyRange.range(_2, _3)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testCount() {
    Integer count = db.scan()
      .flatMap(Observable::from)
      .count().toBlocking().first();
    assertThat(count).isEqualTo(9);
  }

  @Test
  public void testScanSingleRange() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __1);
    toStreamBlocking(db.scan(KeyRange.range(_1, _1)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanOnly() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __2);
    toStreamBlocking(db.scan(KeyRange.onlyByte(2)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanRangeBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __2);
    toStreamBlocking(db.scan(KeyRange.range(_3, _2)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtLeast() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __9);
    toStreamBlocking(db.scan(KeyRange.atLeast(_3)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtLeastBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __1);
    toStreamBlocking(db.scan(KeyRange.atLeastBackward(__3)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtMost() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __4);
    toStreamBlocking(db.scan(KeyRange.atMost(_4)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtMostBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__9, __4);
    toStreamBlocking(db.scan(KeyRange.atMostBackward(_4)))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanForward() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan(KeyRange.forward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanForwardUnsubscribe() throws InterruptedException {
    LinkedList<KeyValue> expected = Fixture.range(__1, __6);
    toStreamBlocking(db.scan(1, KeyRange.forward()).takeWhile(kvs -> {
      assertThat(kvs.size()).isEqualTo(1);
      byte[] key = expected.pollFirst().key;
      assertThat(key).isEqualTo(kvs.get(0).key);
      return key[0] < 6;
    }));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanForwardUnsubscribeBeforeBufferDrained() throws InterruptedException {
    LinkedList<KeyValue> expected = Fixture.range(__1, __6);
    toStreamBlocking(db.scan(KeyRange.forward()).takeWhile(kvs -> {
      KeyValue kv = null;
      for (int i = 0; i < kvs.size(); i++) {
        kv = kvs.get(i);
        if (kv.key[0] > 6) {
          return false;
        }
        assertThat(kv.key).isEqualTo(expected.pollFirst().key);
      }
      return true;
    }));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__9, __1);
    toStreamBlocking(db.scan(KeyRange.backward()))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanBackwardUnsubscribe() throws InterruptedException {
    LinkedList<KeyValue> expected = Fixture.range(__9, __6);
    toStreamBlocking(db.scan(1, KeyRange.backward()).takeWhile(kvs -> {
      assertThat(kvs.size()).isEqualTo(1);
      byte[] key = expected.pollFirst().key;
      assertThat(key).isEqualTo(kvs.get(0).key);
      return key[0] > 6;
    }));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanBackwardUnsubscribeBeforeBufferDrained() throws InterruptedException {
    LinkedList<KeyValue> expected = Fixture.range(__9, __6);
    toStreamBlocking(db.scan(KeyRange.backward()).takeWhile(kvs -> {
      KeyValue kv = null;
      for (int i = 0; i < kvs.size(); i++) {
        kv = kvs.get(i);
        if (kv.key[0] < 6) {
          return false;
        }
        assertThat(kv.key).isEqualTo(expected.pollFirst().key);
      }
      return true;
    }));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test(expected = NoSuchElementException.class)
  public void testScanEmptyRange() {
     db.scan(KeyRange.range(new byte[]{12}, new byte[]{123})).toBlocking().first();
  }

  @Test
  public void testScanMapper() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    toStreamBlocking(db.scan((key, value) -> key.getByte(0)))
      .forEach(k -> assertThat(expected.pollFirst().key[0]).isEqualTo(k));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test(expected = NoSuchElementException.class)
  public void testScanMapperNull() {
    db.scan((key, value) -> null).toBlocking().first();
  }
}
