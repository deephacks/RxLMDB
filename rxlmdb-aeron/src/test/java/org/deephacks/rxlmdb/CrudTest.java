package org.deephacks.rxlmdb;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class CrudTest implements Base {
  static {
//    Base.setDebugMode();
  }
  static RxLmdbServer server;
  static RxLmdbClient client;

  @BeforeClass
  public static void beforeClass() {
    server = RxLmdbServer.builder().build();
    client = RxLmdbClient.builder().build().connectAndWait();
  }

  @AfterClass
  public static void afterClass() throws Exception {
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
    assertNull(client.get(new byte[] {9,9,9}).toBlocking().firstOrDefault(null));
  }

  @Test
  public void getEmptyKey() {
    assertNull(client.get(new byte[0]).toBlocking().firstOrDefault(null));
  }

  @Test
  public void testBatch() throws InterruptedException {
    List<KeyValue> kvs = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KeyValue kv = Fixture.kv("batch", i);
      kvs.add(kv);
      client.batch(kv);
    }

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
