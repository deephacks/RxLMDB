package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.UUID;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.junit.Assert.assertTrue;

public class ScanTest {
  RxDB db;
  Observable<KeyValue> obs = Observable.from(oneToFive);

  @Before
  public void before() {
    db = RxDB.tmp();
    db.put(obs);
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testScanRangeForward() {
    LinkedList<KeyValue> expected = Fixture.range(_2, _3);
    db.scan(KeyRange.range(_2, _3)).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanRangeBackward() {
    LinkedList<KeyValue> expected = Fixture.range(_3, _2);
    db.scan(KeyRange.rangeBackward(_3, _2)).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtLeast() {
    LinkedList<KeyValue> expected = Fixture.range(_3, _5);
    db.scan(KeyRange.atLeast(_3)).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtLeastBackward() {
    LinkedList<KeyValue> expected = Fixture.range(_3, _1);
    db.scan(KeyRange.atLeastBackward(_3)).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtMost() {
    LinkedList<KeyValue> expected = Fixture.range(_1, _4);
    db.scan(KeyRange.atMost(_4)).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanAtMostBackward() {
    LinkedList<KeyValue> expected = Fixture.range(_5, _4);
    db.scan(KeyRange.atMostBackward(_4)).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanForward() {
    LinkedList<KeyValue> expected = Fixture.range(_1, _5);
    db.scan(KeyRange.forward()).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }

  @Test
  public void testScanBackward() {
    LinkedList<KeyValue> expected = Fixture.range(_5, _1);
    db.scan(KeyRange.backward()).forEach(kv -> assertThat(expected.pollFirst().key).isEqualTo(kv.key));
    assertTrue(expected.isEmpty());
  }
}
