package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.junit.Assert.assertTrue;

public class ScanTest {
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
  public void testScanRangeForward() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __3);
    db.scan(KeyRange.range(_2, _3))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    System.out.println(expected);
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanRangeBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __2);
    db.scan(KeyRange.range(_3, _2))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    System.out.println(expected);
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtLeast() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __9);
    System.out.println(expected);
    db.scan(KeyRange.atLeast(_3))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtLeastBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __1);
    db.scan(KeyRange.atLeastBackward(__3))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtMost() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __4);
    db.scan(KeyRange.atMost(_4))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    System.out.println(expected);
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtMostBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__9, __4);
    db.scan(KeyRange.atMostBackward(_4))
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanForward() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    db.scan(KeyRange.forward())
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__9, __1);
    db.scan(KeyRange.backward())
      .forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanMapper() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    db.scan((key, value) -> key.getByte(0))
      .forEach(k -> assertThat(expected.pollFirst().key[0]).isEqualTo(k));
    assertTrue(expected.isEmpty());
  }

  @Test(expected = NoSuchElementException.class)
  public void testScanMapperNull() {
    db.scan((key, value) -> null).toBlocking().first();
  }
}
