package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.*;
import rx.Observable;
import rx.Subscriber;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RxLMDB {
  final Env env;
  final Path path;

  private RxLMDB(Builder builder) {
    this.env = new Env();
    Optional.ofNullable(builder.size)
      .ifPresent(size -> this.env.setMapSize(size));
    this.path = IoUtil.createPathOrTemp(builder.path);
    this.env.open(path.toString());
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates an environment of 64MB in a temporary location.
   */
  public static RxLMDB tmp() {
    return new Builder().size(ByteUnit.MEGA, 64).build();
  }

  public Path getPath() {
    return path;
  }

  public long getSize() {
    return env.info().getMapSize();
  }

  public void close() {
    env.close();
  }

  public RxTx writeTx() {
    return new RxTx(env.createWriteTransaction());
  }

  public RxTx readTx() {
    return new RxTx(env.createReadTransaction());
  }

  public static class Builder {
    private Optional<Path> path = Optional.empty();
    private long size;

    public Builder path(String path) {
      this.path = Optional.ofNullable(Paths.get(path));
      return this;
    }

    public Builder size(ByteUnit unit, long size) {
      this.size = unit.toBytes(size);
      return this;
    }

    public RxLMDB build() {
      return new RxLMDB(this);
    }
  }
}
