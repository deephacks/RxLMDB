package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ConcurrencyTest {
  RxDbGrpcServer server;
  RxDbGrpcClient client = RxDbGrpcClient.builder().build();
  ExecutorService service;

  @Before
  public void before() throws IOException {
    server = RxDbGrpcServer.builder().build();
    client = RxDbGrpcClient.builder().build();
    service = Executors.newCachedThreadPool();
  }

  @After
  public void after() throws Exception {
    client.close();
    server.close();
    service.shutdownNow();
  }

  @Test
  public void testMultipleThreadsWithSharedConnection() throws Exception {
    CountDownLatch latch = new CountDownLatch(1000);
    AtomicInteger value = new AtomicInteger();
    for (int i = 0; i < 1000; i++) {
      service.execute(() -> {
        int k = value.incrementAndGet();
        client.put(Fixture.kv(k, k)).toBlocking().first();
        latch.countDown();
      });
    }
    latch.await();
    Integer count = client.scan().count().toBlocking().first();
    assertThat(count, is(1000));
  }

  @Test
  public void testMultipleThreadsWithSeparateConnections() throws Exception {
    RxDbGrpcClient client2 = RxDbGrpcClient.builder().build();
    CountDownLatch latch = new CountDownLatch(1000);
    AtomicInteger value = new AtomicInteger();
    for (int i = 0; i < 1000; i++) {
      service.execute(() -> {
        int k = value.incrementAndGet();
        RxDbGrpcClient flipFlop = k % 2 == 0 ? client : client2;
        flipFlop.put(Fixture.kv(k, k)).toBlocking().first();
        latch.countDown();
      });
    }
    latch.await();
    Integer count = client.scan().count().toBlocking().first();
    assertThat(count, is(1000));
    client2.close();
  }
}
