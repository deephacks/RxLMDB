package org.deephacks.rxlmdb;

import io.reactivesocket.aeron.server.ReactiveSocketAeronServer;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.util.Optional;

public class RxLmdbServer {
  static {
    System.setProperty("reactivesocket.aeron.clientConcurrency", "1");
  }

  private final int port;
  private final String host;

  private ReactiveSocketAeronServer server;
  private RxLmdb lmdb;
  private RxDb db;
  private MediaDriver mediaDriver;
  private final boolean manageMediaDriver;

  private RxLmdbServer(Builder builder) {
    this.port = Optional.ofNullable(builder.port).orElse(Consts.DEFAULT_PORT);
    this.host = Optional.ofNullable(builder.host).orElse("localhost");
    this.lmdb = Optional.ofNullable(builder.lmdb).orElseGet(() -> RxLmdb.tmp());
    this.db = Optional.ofNullable(builder.db).orElseGet(() -> RxLmdbServer.this.lmdb.dbBuilder().build());
    this.manageMediaDriver = Optional.ofNullable(builder.manageMediaDriver).orElse(true);
    if (this.manageMediaDriver) {
      startMediaDriver();
    }
    server = ReactiveSocketAeronServer.create(host, port, setupPayload -> {
      return new RxLmdbRequestHandler(db);
    });
  }

  public void startMediaDriver() {
    ThreadingMode threadingMode = ThreadingMode.SHARED;

    boolean dedicated = Boolean.getBoolean("dedicated");

    if (dedicated) {
      threadingMode = ThreadingMode.DEDICATED;
    }

    System.out.println("ThreadingMode => " + threadingMode);

    final MediaDriver.Context ctx = new MediaDriver.Context()
      .threadingMode(threadingMode)
      .dirsDeleteOnStart(true)
      .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 100, 1000))
      .receiverIdleStrategy(new NoOpIdleStrategy())
      .senderIdleStrategy(new NoOpIdleStrategy());

    this.mediaDriver = MediaDriver.launch(ctx);
  }

  public void closeMediaDriver() {
    mediaDriver.close();
  }

  public void close() throws Exception {
    server.close();
    if (this.manageMediaDriver) {
      closeMediaDriver();
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Integer port;
    private String host;
    private RxLmdb lmdb;
    private RxDb db;
    private Boolean manageMediaDriver;

    public RxLmdbServer build() {
      return new RxLmdbServer(this);
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

    public Builder manageMediaDriver(boolean shouldManage) {
      this.manageMediaDriver = shouldManage;
      return this;
    }

  }
}
