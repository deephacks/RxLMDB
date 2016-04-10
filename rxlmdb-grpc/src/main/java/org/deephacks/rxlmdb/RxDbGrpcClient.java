package org.deephacks.rxlmdb;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteStrings;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.deephacks.rxlmdb.DatabaseServiceGrpc.DatabaseServiceStub;
import org.deephacks.rxlmdb.Rxdb.*;
import rx.Observable;
import rx.Subscriber;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class RxDbGrpcClient {
  private final InetSocketAddress address;
  private final ManagedChannelImpl channel;
  private final DatabaseServiceStub stub;

  private RxDbGrpcClient(Builder builder) {
    String host = Optional.ofNullable(builder.host).orElse("localhost");
    int port = Optional.ofNullable(builder.port).orElse(RxDbGrpcServer.DEFAULT_PORT);
    this.address = new InetSocketAddress(host, port);
    this.channel = NettyChannelBuilder.forAddress(this.address)
      .flowControlWindow(65 * 1024)
      .negotiationType(NegotiationType.PLAINTEXT)
      .build();
    this.stub = DatabaseServiceGrpc.newStub(channel);
  }

  public Observable<Boolean> put(KeyValue kv) {
    if (kv == null || kv.key == null || kv.key.length == 0) {
      return Observable.just(false);
    }
    return Observable.create(new Observable.OnSubscribe<Boolean>() {
      @Override
      public void call(Subscriber<? super Boolean> subscriber) {
        ByteString k = UnsafeByteStrings.unsafeWrap(ByteBuffer.wrap(kv.key));
        ByteString v = UnsafeByteStrings.unsafeWrap(ByteBuffer.wrap(kv.value));
        PutMsg put = PutMsg.newBuilder().setKey(k).setVal(v).build();
        stub.put(put, new StreamObserver<Empty>() {
          @Override
          public void onNext(Empty value) {
          }

          @Override
          public void onError(Throwable t) {
            subscriber.onError(t);
          }

          @Override
          public void onCompleted() {
            subscriber.onNext(true);
            subscriber.onCompleted();
          }
        });
      }
    });
  }

  public void batch(Observable<List<KeyValue>> values) {
    if (values == null) {
      return;
    }
    StreamObserver<PutMsg> batch = stub.batch(new StreamObserver<Empty>() {
      @Override
      public void onNext(Empty value) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });

    values.subscribe(new Subscriber<List<KeyValue>>() {
      @Override
      public void onCompleted() {
        batch.onCompleted();
      }

      @Override
      public void onError(Throwable throwable) {
        batch.onError(throwable);
      }

      @Override
      public void onNext(List<KeyValue> kvs) {
        for (KeyValue kv : kvs) {
          ByteString k = UnsafeByteStrings.unsafeWrap(ByteBuffer.wrap(kv.key));
          ByteString v = UnsafeByteStrings.unsafeWrap(ByteBuffer.wrap(kv.value));
          PutMsg put = PutMsg.newBuilder().setKey(k).setVal(v).build();
          batch.onNext(put);
        }
      }
    });
  }

  public Observable<Boolean> delete(byte[] key) {
    if (key == null || key.length == 0) {
      return Observable.just(false);
    }
    return Observable.create(new Observable.OnSubscribe<Boolean>() {
      @Override
      public void call(Subscriber<? super Boolean> subscriber) {
        ByteString k = UnsafeByteStrings.unsafeWrap(ByteBuffer.wrap(key));
        DeleteMsg delete = DeleteMsg.newBuilder().setKey(k).build();
        stub.delete(delete, new StreamObserver<BooleanMsg>() {
          @Override
          public void onNext(BooleanMsg value) {
            subscriber.onNext(value.getValue());
          }

          @Override
          public void onError(Throwable t) {
            subscriber.onError(t);
          }

          @Override
          public void onCompleted() {
            subscriber.onCompleted();
          }
        });
      }
    });
  }

  public Observable<KeyValue> get(byte[] key) {
    if (key == null || key.length == 0) {
      return Observable.just(null);
    }
    return Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        ByteString k = UnsafeByteStrings.unsafeWrap(ByteBuffer.wrap(key));
        GetMsg get = GetMsg.newBuilder().setKey(k).build();
        stub.get(get, new StreamObserver<ValueMsg>() {
          @Override
          public void onNext(ValueMsg value) {
            if (!value.getVal().isEmpty()) {
              KeyValue kv = new KeyValue(key, value.getVal().toByteArray());
              subscriber.onNext(kv);
            }
          }

          @Override
          public void onError(Throwable t) {
            subscriber.onError(t);
          }

          @Override
          public void onCompleted() {
            subscriber.onCompleted();
          }
        });
      }
    });
  }

  public Observable<KeyValue> scan() {
    return scan(KeyRange.forward());
  }

  public Observable<KeyValue> scan(KeyRange range) {
    if (range == null) {
      return Observable.just(null);
    }
    return Observable.create(new Observable.OnSubscribe<KeyValue>() {
      @Override
      public void call(Subscriber<? super KeyValue> subscriber) {
        KeyRangeMsg.Builder builder = KeyRangeMsg.newBuilder();
        if (range.start != null) {
          builder.setStart(ByteString.copyFrom(range.start));
        }
        if (range.stop != null) {
          builder.setStop(ByteString.copyFrom(range.stop));
        }
        builder.setTypeValue(range.type.ordinal());
        stub.scan(builder.build(), new StreamObserver<KeyValueMsg>() {
          @Override
          public void onNext(KeyValueMsg msg) {
            KeyValue kv = new KeyValue(msg.getKey().toByteArray(), msg.getVal().toByteArray());
            subscriber.onNext(kv);
          }

          @Override
          public void onError(Throwable t) {
            subscriber.onError(t);
          }

          @Override
          public void onCompleted() {
            subscriber.onCompleted();
          }
        });
      }
    });
  }

  public void close() throws Exception {
    this.channel.shutdown();
    this.channel.awaitTermination(10, TimeUnit.SECONDS);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Integer port;
    private String host;

    public RxDbGrpcClient build() {
      return new RxDbGrpcClient(this);
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
