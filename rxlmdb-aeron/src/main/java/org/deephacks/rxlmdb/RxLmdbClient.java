package org.deephacks.rxlmdb;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.client.AeronClientDuplexConnection;
import io.reactivesocket.aeron.client.AeronClientDuplexConnectionFactory;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Optional;

public class RxLmdbClient {
  static {
    System.setProperty("reactivesocket.aeron.clientConcurrency", "1");
  }

  private ReactiveSocket reactiveSocket;
  private AeronClientDuplexConnection connection;
  private final InetSocketAddress address;
  private final AeronClientDuplexConnectionFactory cf;

  private RxLmdbClient(Builder builder) {
    String host = Optional.ofNullable(builder.host).orElse("localhost");
    int port =  Optional.ofNullable(builder.port).orElse(Consts.DEFAULT_PORT);
    this.address = new InetSocketAddress(host, port);
    this.cf = AeronClientDuplexConnectionFactory.getInstance();
    cf.addSocketAddressToHandleResponses(address);
  }

  public RxLmdbClient connectAndWait() {
    Publisher<AeronClientDuplexConnection> udpConnection = cf.createAeronClientDuplexConnection(address);
    connection = RxReactiveStreams.toObservable(udpConnection)
      .toBlocking().single();
    ConnectionSetupPayload setup = ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS);
    reactiveSocket = ReactiveSocket.fromClientConnection(connection, setup);
    reactiveSocket.startAndWait();
    return this;
  }

  public void batch(KeyValue kv) {
    if (kv == null || kv.key == null || kv.key.length == 0) {
      return;
    }
    KeyValuePayload kvp = new KeyValuePayload(kv, OpType.PUT);
    RxReactiveStreams.toObservable(reactiveSocket.fireAndForget(kvp))
      .map(payload -> {
        kvp.release();
        return null;
      }).subscribe();
  }

  public Observable<Boolean> put(KeyValue kv) {
    if (kv == null || kv.key == null || kv.key.length == 0) {
      return Observable.just(false);
    }
    KeyValuePayload kvp = new KeyValuePayload(kv, OpType.PUT);
    return RxReactiveStreams.toObservable(reactiveSocket.requestResponse(kvp))
      .map(payload -> {
        kvp.release();
        return Boolean.TRUE;
      });
  }

  public Observable<Boolean> delete(byte[] key) {
    if (key == null || key.length == 0) {
      return Observable.just(false);
    }
    KeyValuePayload kvp = new KeyValuePayload(key, OpType.DELETE);
    return RxReactiveStreams.toObservable(reactiveSocket.requestResponse(kvp))
      .map(payload -> {
        kvp.release();
        byte[] response = KeyValuePayload.getByteArray(payload);
        return Arrays.equals(key, response);
      });
  }

  public Observable<KeyValue> get(byte[] key) {
    if (key == null || key.length == 0) {
      return Observable.just(null);
    }
    KeyValuePayload kvp = new KeyValuePayload(key, OpType.GET);
    return RxReactiveStreams
      .toObservable(reactiveSocket.requestResponse(kvp))
      .map(payload -> {
        kvp.release();
        return KeyValuePayload.getKeyValue(payload);
      });
  }

  public Observable<KeyValue> scan() {
    ScanPayload scan = new ScanPayload();
    return RxReactiveStreams
      .toObservable(reactiveSocket.requestStream(scan))
      .map(payload -> {
        KeyValue kv = KeyValuePayload.getKeyValue(payload);
        scan.release();
        return kv;
      });
  }

  public void close() throws Exception {
    reactiveSocket.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Integer port;
    private String host;

    public RxLmdbClient build() {
      return new RxLmdbClient(this);
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }
  }
}
