package org.deephacks.rxlmdb;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class CrudTest implements Base {

  static RxDbGrpcServer server;
  static RxDbGrpcClient client;

  @BeforeClass
  public static void beforeClass() throws IOException {
    server = RxDbGrpcServer.builder().build();
    client = RxDbGrpcClient.builder().build();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    client.close();
    server.close();
  }

  @Test
  public void testPutGetDelete() {
    KeyValue kv1 = Fixture.kv("putGetDelete", 1);
    assertTrue(client.put(kv1).toBlocking().first());
    KeyValue kv2 = null;
    for (int i = 0; i < 100; i++) {
      kv2 = client.get(kv1.key).toBlocking().first();
    }
    assertArrayEquals(kv1.key, kv2.key);
    assertArrayEquals(kv1.value, kv2.value);

    assertTrue(client.delete(kv1.key).toBlocking().first());
    assertNull(client.get(kv1.key).toBlocking().firstOrDefault(null));
  }

  @Test
  public void deleteNonExisting() {
    assertFalse(client.delete(new byte[] {9,9,9}).toBlocking().first());
  }

  @Test
  public void getNonExisting() {
    KeyValue keyValue = client.get(new byte[]{9, 9, 9}).toBlocking().firstOrDefault(null);
    assertNull(keyValue);
  }

  @Test
  public void getEmptyKey() {
    assertNull(client.get(new byte[0]).toBlocking().firstOrDefault(null));
  }

  @Test
  public void testBatch() throws InterruptedException {
    List<KeyValue> kvs = new ArrayList<>();
    SerializedSubject<KeyValue, KeyValue> subject = PublishSubject.<KeyValue>create().toSerialized();
    client.batch(subject.buffer(10, TimeUnit.NANOSECONDS, 512));

    for (int i = 0; i < 1000; i++) {
      KeyValue kv = Fixture.kv("batch", i);
      kvs.add(kv);
      subject.onNext(kv);
    }
    subject.onCompleted();
    Thread.sleep(1000);

    List<KeyValue> list = client.scan().filter(keyPrefix("batch"))
      .toList().toBlocking().first();
    assertThat(list.size(), is(1000));
    for (int i = 0; i < 1000; i++) {
      KeyValue kv = kvs.get(i);
      assertThat(new String(kv.key), is(new String(list.get(i).key)));
    }
  }

  @Test
  public void testScan() throws InterruptedException {
    List<KeyValue> kvs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      KeyValue kv = Fixture.kv("scan", i);
      kvs.add(kv);
      assertTrue(client.put(kv).toBlocking().first());
    }
    List<KeyValue> list = client.scan().filter(keyPrefix("scan"))
      .toList().toBlocking().first();
    assertThat(list.size(), is(100));
    for (int i = 0; i < 100; i++) {
      KeyValue kv = kvs.get(i);
      assertThat(new String(kv.key), is(new String(list.get(i).key)));
    }
  }
}
