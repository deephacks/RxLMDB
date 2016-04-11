package org.deephacks.rxlmdb;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ConnectivityTest implements Base {

  @Test
  public void testConnectClose() throws Exception {
    for (int i = 0; i < 10; i++) {
      RxDbGrpcServer server = RxDbGrpcServer.builder().build();
      RxDbGrpcClient client = RxDbGrpcClient.builder().build();
      client.close();
      server.close();
    }
  }

  @Test
  public void testServerCloseThenReconnectClient() throws Exception {
    RxLmdb lmdb = RxLmdb.tmp();
    Path path = lmdb.getPath();
    RxDbGrpcServer server = RxDbGrpcServer.builder().lmdb(lmdb).build();
    RxDbGrpcClient client = RxDbGrpcClient.builder().build();
    KeyValue kv = Fixture.values[0];
    try {
      client.put(kv).toBlocking().first();
      server.close();
      client.get(kv.key).toBlocking().first();
      fail("should throw");
    } catch (StatusRuntimeException e) {
      assertThat(e.getStatus().getCode(), is(Status.UNAVAILABLE.getCode()));
    }
    lmdb = RxLmdb.builder().path(path).build();
    server = RxDbGrpcServer.builder().lmdb(lmdb).build();
    client.get(kv.key).toBlocking().first();
    server.close();
  }

  @Test
  public void testClientTimeout() {
  }

  @Test
  public void testServerTimeout() {
  }

}
