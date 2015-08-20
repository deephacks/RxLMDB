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
import rx.*;
import rx.exceptions.OnErrorFailedException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class RxDB {
  /** copy by default */
  final DirectMapper.KeyValueMapper KV_MAPPER = new DirectMapper.KeyValueMapper();
  final RxLMDB lmdb;
  final Database db;
  final String name;
  final Scheduler scheduler;
  final int defaultBuffer = 512;

  private RxDB(Builder builder) {
    this.lmdb = builder.lmdb;
    this.name = Optional.ofNullable(builder.name).orElse("default");
    this.db = lmdb.env.openDatabase(this.name);
    this.scheduler = lmdb.scheduler;
  }

  public void put(Observable<KeyValue> values) {
    put(lmdb.internalWriteTx(), values);
  }

  public void put(RxTx tx, Observable<KeyValue> values) {
    put(tx, values, false);
  }

  public void append(Observable<KeyValue> values) {
    append(lmdb.internalWriteTx(), values);
  }

  public void append(RxTx tx, Observable<KeyValue> values) {
    put(tx, values, true);
  }

  private void put(RxTx tx, Observable<KeyValue> values, boolean append) {
    PutSubscriber putSubscriber = new PutSubscriber(this, tx, append);
    values.subscribe(putSubscriber);
  }

  public Observable<KeyValue> get(Observable<byte[]> keys) {
    return get(lmdb.internalReadTx(), KV_MAPPER, keys);
  }

  public Observable<KeyValue> get(RxTx tx, Observable<byte[]> keys) {
    return get(tx, KV_MAPPER, keys);
  }

  public <T> Observable<T> get(DirectMapper<T> mapper, Observable<byte[]> keys) {
    return get(lmdb.internalReadTx(), mapper, keys);
  }

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

  /**
   * @see org.deephacks.rxlmdb.RxDB#delete(RxTx)
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
        .map(kv -> Observable.just(kv.key))
        .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
      delete(tx, keys);
    });
  }

  /**
   * @see org.deephacks.rxlmdb.RxDB#delete(RxTx, Observable)
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

  public <T> Observable<List<T>> scan(int buffer, DirectMapper<T> mapper) {
    return scan(buffer, lmdb.internalReadTx(), mapper);
  }

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

  public static RxDB tmp() {
    return new Builder().lmdb(RxLMDB.tmp()).build();
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
    private RxLMDB lmdb;

    public Builder lmdb(RxLMDB lmdb) {
      this.lmdb = lmdb;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public RxDB build() {
      return new RxDB(this);
    }
  }

  private static class PutSubscriber extends Subscriber<KeyValue> {
    final RxTx tx;
    final Database db;
    final boolean append;

    private PutSubscriber(RxDB db, RxTx tx, boolean append) {
      this.tx = tx;
      this.db = db.db;
      this.append = append;
    }

    @Override
    public void onCompleted() {
      if (!tx.isUserManaged) {
        tx.commit();
      }
    }

    @Override
    public void onError(Throwable e) {
      tx.abort();
    }

    @Override
    public void onNext(KeyValue kv) {
      try {
        db.put(tx.tx, kv.key, kv.value, append ? Constants.APPEND : 0);
      } catch (Throwable e) {
        throw new OnErrorFailedException(e);
      }
    }
  }

  private static class DeleteSubscriber extends Subscriber<byte[]> {
    final RxTx tx;
    final Database db;

    private DeleteSubscriber(RxDB db, RxTx tx) {
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
