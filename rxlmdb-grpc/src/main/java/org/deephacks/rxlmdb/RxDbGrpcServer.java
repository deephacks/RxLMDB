package org.deephacks.rxlmdb;

import io.grpc.ServerInterceptors;
import io.grpc.internal.ServerImpl;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.Optional;

public class RxDbGrpcServer {
  static final int DEFAULT_PORT = 9098;
  private final int port;
  private final String host;
  private RxLmdb lmdb;
  private RxDb db;
  private ServerImpl server;

  private RxDbGrpcServer(Builder builder) throws IOException {
    this.port = Optional.ofNullable(builder.port).orElse(DEFAULT_PORT);
    this.host = Optional.ofNullable(builder.host).orElse("localhost");
    this.lmdb = Optional.ofNullable(builder.lmdb).orElseGet(() -> RxLmdb.tmp());
    this.db = Optional.ofNullable(builder.db).orElseGet(() -> RxDbGrpcServer.this.lmdb.dbBuilder().build());
    this.server = NettyServerBuilder.forPort(port)
      .addService(ServerInterceptors.intercept(
        DatabaseServiceGrpc.bindService(new RxDbServiceGrpc(this.db))))
      .build().start();
  }

  public void close() throws Exception {
    server.shutdown();
    server.awaitTermination();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Integer port;
    private String host;
    private RxLmdb lmdb;
    private RxDb db;

    public RxDbGrpcServer build() throws IOException {
      return new RxDbGrpcServer(this);
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder lmdb(RxLmdb lmdb) {
      this.lmdb = lmdb;
      return this;
    }

    public Builder db(RxDb db) {
      this.db = db;
      return this;
    }
  }
}
