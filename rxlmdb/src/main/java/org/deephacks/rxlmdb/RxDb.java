/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.*;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.exceptions.OnErrorFailedException;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class RxDb {
  /** copy by default */
  final DirectMapper.KeyValueMapper KV_MAPPER = new DirectMapper.KeyValueMapper();
  final RxLmdb lmdb;
  final Database db;
  final String name;
  final Scheduler scheduler;
  final int defaultBuffer = 512;

  private RxDb(Builder builder) {
    this.lmdb = builder.lmdb;
    this.name = Optional.ofNullable(builder.name).orElse("default");
    this.db = lmdb.env.openDatabase(this.name);
    this.scheduler = lmdb.scheduler;
  }

  /**
   * Put kvs into the database and commit when the last
   * element has been written.
   */
  public Observable<Boolean> put(Observable<KeyValue> values) {
    return put(lmdb.internalWriteTx(), values);
  }

  public Boolean put(KeyValue kv) {
    db.put(kv.key(), kv.value());
    return true;
  }

  public Boolean put(RxTx tx, KeyValue kv) {
    db.put(tx.tx, kv.key(), kv.value());
    return true;
  }

  /**
   * Same as regular put but the user is in charge of the transaction.
   *
   * @see RxDb#put(Observable)
   */
  public Observable<Boolean> put(RxTx tx, Observable<KeyValue> values) {
    return put(tx, values, false);
  }

  /**
   * @see RxDb#append(RxTx, Observable)
   */
  public void append(Observable<KeyValue> values) {
    append(lmdb.internalWriteTx(), values);
  }

  /**
   * Append the kvs to the end of the database without comparing its order first.
   * Appending a key that is not greater than the highest existing key will
   * cause corruption.
   */
  public void append(RxTx tx, Observable<KeyValue> values) {
    put(tx, values, true);
  }

  public void append(RxTx tx, KeyValue kv) {
    db.put(tx.tx, kv.key(), kv.value(), Constants.APPEND);
  }

  public void append(KeyValue kv) {
    db.put(kv.key(), kv.value(), Constants.APPEND);
  }

  /**
   * Write and commit kvs asynchronously in batches. The user is free to
   * use choose whatever buffering configuration is needed.
   */
  public void batch(Observable<List<KeyValue>> values) {
    BatchSubscriber putSubscriber = new BatchSubscriber(this);
    values.subscribe(putSubscriber);
  }

  private Observable<Boolean> put(RxTx tx, Observable<KeyValue> values, boolean append) {
    PutSubscriber putSubscriber = new PutSubscriber(this, tx, append);
    values.subscribe(putSubscriber);
    return putSubscriber.result;
  }

  /**
   * Get kvs from the database. Items that are not found will be
   * represented as null.
   */
  public Observable<KeyValue> get(Observable<byte[]> keys) {
    return get(lmdb.internalReadTx(), KV_MAPPER, keys);
  }

  /**
   * @see RxDb#get(Observable)
   */
  public Observable<KeyValue> get(RxTx tx, Observable<byte[]> keys) {
    return get(tx, KV_MAPPER, keys);
  }

  /**
   * Allow zero copy transformation of the resulting values.
   *
   * @see RxDb#get(Observable)
   */
  public <T> Observable<T> get(DirectMapper<T> mapper, Observable<byte[]> keys) {
    return get(lmdb.internalReadTx(), mapper, keys);
  }

  /**
   * Allow zero copy transformation of the resulting values.
   *
   * @see RxDb#get(Observable)
   */
  public <T> Observable<T> get(RxTx tx, DirectMapper<T> mapper, Observable<byte[]> keys) {
    return keys.flatMap(key -> {
      try {
        ByteBuffer bb = ByteBuffer.allocateDirect(key.length).put(key);
        DirectBuffer keyBuffer = new DirectBuffer(bb);
        DirectBuffer valBuffer = new DirectBuffer(0, 0);
        if (LMDBException.NOTFOUND != db.get(tx.tx, keyBuffer, valBuffer)) {
          return Observable.just(mapper.map(keyBuffer, valBuffer));
        } else {
          return Observable.just(null);
        }
      } catch (Throwable e) {
        return Observable.just(null);
      }
    }).doOnCompleted(() -> {
      if (!tx.isUserManaged) {
        tx.commit();
      }
    });
  }

  public byte[] get(byte[] key) {
    return db.get(key);
  }

  public byte[] get(RxTx tx, byte[] key) {
    return db.get(tx.tx, key);
  }

  /**
   * @see RxDb#delete(RxTx)
   */
  public void delete() {
    delete(lmdb.internalWriteTx());
  }

  /**
   * Delete all records
   */
  public void delete(RxTx tx) {
    // non-blocking needed?
    scan(tx).toBlocking().forEach(keyValues -> {
      Observable<byte[]> keys = keyValues.stream()
        .map(kv -> Observable.just(kv.key()))
        .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
      delete(tx, keys);
    });
  }

  /**
   * @see RxDb#delete(RxTx, Observable)
   */
  public void delete(Observable<byte[]> keys) {
    delete(lmdb.internalWriteTx(), keys);
  }

  /**
   * Delete records associated with provided keys.
   */
  public void delete(RxTx tx, Observable<byte[]> keys) {
    keys.subscribe(new DeleteSubscriber(this, tx));
  }

  public <T> Observable<List<T>> scan(DirectMapper<T> scan) {
    return scan(defaultBuffer, lmdb.internalReadTx(), scan);
  }

  public boolean delete(byte[] key) {
    return db.delete(key);
  }

  public boolean delete(RxTx tx, byte[] key) {
    return db.delete(tx.tx, key);
  }

  /**
   * Forward scan the database with a configurable buffer size and the
   * ability to zero copy transformation of the resulting values.
   */
  public <T> Observable<List<T>> scan(int buffer, DirectMapper<T> mapper) {
    return scan(buffer, lmdb.internalReadTx(), mapper);
  }

  /**
   * Scan multiple ranges of kvs in parallel.
   */
  public Observable<List<KeyValue>> scan(KeyRange... ranges) {
    return scan(defaultBuffer, lmdb.internalReadTx(), KV_MAPPER, ranges);
  }

  public Observable<List<KeyValue>> scan(int buffer, KeyRange... ranges) {
    return scan(buffer, lmdb.internalReadTx(), KV_MAPPER, ranges);
  }

  public Observable<List<KeyValue>> scan(RxTx tx, KeyRange... ranges) {
    return scan(defaultBuffer, tx, KV_MAPPER, ranges);
  }

  public Observable<List<KeyValue>> scan(int buffer, RxTx tx, KeyRange... ranges) {
    return scan(buffer, tx, KV_MAPPER, ranges);
  }

  public <T> Observable<List<T>> scan(DirectMapper<T> mapper, KeyRange... ranges) {
    return scan(defaultBuffer, lmdb.internalReadTx(), mapper, ranges);
  }

  public <T> Observable<List<T>> scan(int buffer, DirectMapper<T> mapper, KeyRange... ranges) {
    return scan(buffer, lmdb.internalReadTx(), mapper, ranges);
  }

  public <T> Observable<List<T>> scan(RxTx tx, DirectMapper<T> mapper, KeyRange... ranges) {
    return scan(defaultBuffer, tx, mapper, ranges);
  }

  public <T> Observable<List<T>> scan(int buffer, RxTx tx, DirectMapper<T> mapper, KeyRange... ranges) {
    return Scanners.scan(db, tx, mapper, scheduler, buffer, ranges);
  }

  public <T> Observable<List<T>> cursor(CursorScanner<T> scanner) {
    return cursor(defaultBuffer, lmdb.internalReadTx(), scanner);
  }

  public <T> Observable<List<T>> cursor(RxTx tx, CursorScanner<T> scanner) {
    return cursor(defaultBuffer, tx, scanner);
  }

  public <T> Observable<List<T>> cursor(int buffer, CursorScanner<T> scanner) {
    return cursor(buffer, lmdb.internalReadTx(), scanner);
  }

  public <T> Observable<List<T>> cursor(int buffer, RxTx tx, CursorScanner<T> scanner) {
    return Scanners.scan(db, buffer, tx, scanner);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static RxDb tmp() {
    return new Builder().lmdb(RxLmdb.tmp()).build();
  }

  public String getName() {
    return name;
  }

  public long getSize() {
    return lmdb.env.info().getMapSize();
  }

  public void close() {
    db.close();
  }

  public static class Builder {
    private String name;
    private RxLmdb lmdb;

    public Builder lmdb(RxLmdb lmdb) {
      this.lmdb = lmdb;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public RxDb build() {
      return new RxDb(this);
    }
  }

  private static class PutSubscriber extends Subscriber<KeyValue> implements Loggable {
    final RxTx tx;
    final Database db;
    final boolean append;
    final PublishSubject<Boolean> result;

    private PutSubscriber(RxDb db, RxTx tx, boolean append) {
      this.tx = tx;
      this.db = db.db;
      this.append = append;
      this.result = PublishSubject.create();
    }

    @Override
    public void onCompleted() {
      if (!tx.isUserManaged) {
        tx.commit();
      }
      result.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
      tx.abort();
      logger().error("Put error.", e);
      result.onError(e);
    }

    @Override
    public void onNext(KeyValue kv) {
      try {
        db.put(tx.tx, kv.key(), kv.value(), append ? Constants.APPEND : 0);
        result.onNext(true);
      } catch (Throwable e) {
        if (e instanceof RuntimeException) {
          throw e;
        }
        throw new OnErrorFailedException(e);
      }
    }
  }

  private static class BatchSubscriber extends Subscriber<List<KeyValue>> implements Loggable {
    final Database db;
    final Env env;

    private BatchSubscriber(RxDb db) {
      this.env = db.lmdb.env;
      this.db = db.db;
    }

    @Override
    public void onCompleted() {
    }

    @Override
    public void onError(Throwable e) {
      logger().error("Batch error.", e);
    }

    @Override
    public void onNext(List<KeyValue> kvs) {
      try {
        if (kvs.size() < 1) {
          return;
        }
        try (Transaction tx = env.createWriteTransaction()) {
          for (KeyValue kv : kvs) {
            try {
              db.put(tx, kv.key(), kv.value(), 0);
            } catch (Throwable e) {
              // log error, swallow exception and proceed to next kv
              logger().error("Batch put error.", e);
            }
          }
          tx.commit();
        }
      } catch (Throwable e) {
        throw new OnErrorFailedException(e);
      }
    }
  }

  private static class DeleteSubscriber extends Subscriber<byte[]> implements Loggable {
    final RxTx tx;
    final Database db;

    private DeleteSubscriber(RxDb db, RxTx tx) {
      this.tx = tx;
      this.db = db.db;
    }

    @Override
    public void onCompleted() {
      if (!tx.isUserManaged) {
        tx.commit();
      }
    }

    @Override
    public void onError(Throwable e) {
      logger().error("Delete error.", e);
      if (!tx.isUserManaged) {
        tx.abort();
      }
    }

    @Override
    public void onNext(byte[] key) {
      db.delete(tx.tx, key);
    }
  }
}
