/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.*;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class RxLMDB {
  final Env env;
  final Path path;
  final Scheduler scheduler;

  private RxLMDB(Builder builder) {
    this.env = new Env();
    Optional.ofNullable(builder.size)
      .ifPresent(size -> this.env.setMapSize(size));
    Optional.ofNullable(builder.maxDbs)
      .ifPresent(size -> this.env.setMaxDbs(builder.maxDbs));
    Optional.ofNullable(builder.maxReaders)
      .ifPresent(size -> this.env.setMaxReaders(builder.maxReaders));
    this.path = IoUtil.createPathOrTemp(builder.path);
    // do not tie transactions to threads since it breaks parallel range scans

    this.env.open(path.toString(), Constants.NOTLS | builder.flags);
    this.scheduler = Optional.ofNullable(builder.scheduler)
      .orElse(Schedulers.io());
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

  public void copy(String path) {
    env.copy(path);
  }

  public void copyCompact(String path) {
    env.copyCompact(path);
  }

  public void sync(boolean force) {
    env.sync(force);
  }

  public void close() {
    env.close();
  }

  public RxTx writeTx() {
    return new RxTx(env.createWriteTransaction(), true);
  }

  public RxTx readTx() {
    return new RxTx(env.createReadTransaction(), true);
  }

  RxTx internalReadTx() {
    return new RxTx(env.createReadTransaction(), false);
  }

  RxTx internalWriteTx() {
    return new RxTx(env.createWriteTransaction(), false);
  }

  public static class Builder {
    private Optional<Path> path = Optional.empty();
    private long size;
    private Scheduler scheduler;
    private int flags;
    private Long maxDbs;
    private Long maxReaders;

    public Builder path(String path) {
      this.path = Optional.ofNullable(Paths.get(path));
      return this;
    }

    public Builder size(ByteUnit unit, long size) {
      this.size = unit.toBytes(size);
      return this;
    }

    public Builder scheduler(Scheduler scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    public Builder fixedmap() {
      flags = flags | Constants.FIXEDMAP;
      return this;
    }

    public Builder nosubdir() {
      flags = flags | Constants.NOSUBDIR;
      return this;
    }

    public Builder readOnly() {
      flags = flags | Constants.RDONLY;
      return this;
    }

    public Builder writeMap() {
      flags = flags | Constants.WRITEMAP;
      return this;
    }

    public Builder noMetaSync() {
      flags = flags | Constants.NOMETASYNC;
      return this;
    }

    public Builder noSync() {
      flags = flags | Constants.NOSYNC;
      return this;
    }

    public Builder mapAsync() {
      flags = flags | Constants.MAPASYNC;
      return this;
    }

    public Builder noLock() {
      flags = flags | Constants.NOLOCK;
      return this;
    }

    public Builder noReadahead() {
      flags = flags | Constants.NORDAHEAD;
      return this;
    }

    public Builder noMemInit() {
      flags = flags | Constants.NOMEMINIT;
      return this;
    }

    public Builder maxDbs(long size) {
      maxDbs = size;
      return this;
    }

    public Builder maxReaders(long size) {
      maxReaders = size;
      return this;
    }

    public RxLMDB build() {
      return new RxLMDB(this);
    }
  }
}
