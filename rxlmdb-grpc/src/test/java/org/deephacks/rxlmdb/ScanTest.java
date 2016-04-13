package org.deephacks.rxlmdb;

import org.junit.*;
import rx.Observable;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;

public class ScanTest {
  static RxDbGrpcServer server;
  static RxDbGrpcClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    server = RxDbGrpcServer.builder().build();
    client = RxDbGrpcClient.builder().build();
    client.batch(Observable.from(_1_to_9).buffer(10));
    Thread.sleep(100);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    client.close();
    server.close();
  }

  @Test
  public void testScanRangeForward() {
    LinkedList<KeyValue> expected = Fixture.range(__2, __3);
    client.scan(KeyRange.range(_2, _3)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testCount() {
    Integer count = client.scan().count().toBlocking().first();
    assertThat(count).isEqualTo(9);
  }

  @Test
  public void testScanSingleRange() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __1);
    client.scan(KeyRange.range(_1, _1)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanOnly() {
    byte[] startStop = new byte[] { (byte) 2 };
    LinkedList<KeyValue> expected = Fixture.range(__2, __2);
    client.scan(KeyRange.range(startStop, startStop)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanRangeBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __2);
    client.scan(KeyRange.range(_3, _2)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtLeast() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __9);
    client.scan(KeyRange.atLeast(_3)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtLeastBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__3, __1);
    client.scan(KeyRange.atLeastBackward(__3)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtMost() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __4);
    client.scan(KeyRange.atMost(_4)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanAtMostBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__9, __4);
    client.scan(KeyRange.atMostBackward(_4)).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanForward() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __9);
    client.scan(KeyRange.forward()).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanForwardUnsubscribe() throws InterruptedException {
    LinkedList<KeyValue> expected = Fixture.range(__1, __6);
    client.scan(KeyRange.forward()).takeWhile(kv -> {
      byte[] key = expected.pollFirst().key();
      assertThat(key).isEqualTo(kv.key());
      return key[0] < 6;
    }).toBlocking().last();
    assertThat(expected).isEqualTo(new LinkedList<>());
  }


  @Test
  public void testScanBackward() {
    LinkedList<KeyValue> expected = Fixture.range(__9, __1);
    client.scan(KeyRange.backward()).toBlocking()
      .forEach(kv -> assertThat(expected.pollFirst().key()).isEqualTo(kv.key()));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanBackwardUnsubscribe() throws InterruptedException {
    LinkedList<KeyValue> expected = Fixture.range(__9, __6);
    client.scan(KeyRange.backward()).takeWhile(kv -> {
      byte[] key = expected.pollFirst().key();
      assertThat(key).isEqualTo(kv.key());
      return key[0] > 6;
    }).toBlocking().last();
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test(expected = NoSuchElementException.class)
  public void testScanEmptyRange() {
    client.scan(KeyRange.range(new byte[]{12}, new byte[]{123})).toBlocking().first();
  }

}
