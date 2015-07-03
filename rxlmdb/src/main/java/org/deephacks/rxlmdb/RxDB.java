/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import rx.functions.Func1;
import rx.observables.BlockingObservable;

import java.util.List;
import java.util.Optional;

public class RxDB {
  final Scan.ScanDefault scanDefault = new Scan.ScanDefault();
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
    PutSubscriber putSubscriber = new PutSubscriber(this, tx);
    values.subscribe(putSubscriber);
  }

  public Observable<byte[]> get(byte[] key) {
    return get(key, lmdb.internalReadTx());
  }

  public Observable<byte[]> get(byte[] key, RxTx tx) {
    return Observable.create(subscriber -> {
        try {
          byte[] value = db.get(tx.tx, key);
          if (value != null) {
            subscriber.onNext(value);
          }
          subscriber.onCompleted();
        } catch (Throwable t) {
          if (!tx.isUserManaged) {
            tx.abort();
          }
          subscriber.onError(t);
        } finally {
          if (!tx.isUserManaged) {
            tx.abort();
          }
        }
      }
    );
  }

  public void delete() {
    delete(lmdb.internalWriteTx());
  }

  public void delete(RxTx tx) {
    // non-blocking needed?
    scan(tx).toBlocking().forEach(keyValues -> {
      Observable<byte[]> keys = keyValues.stream()
        .map(kv -> Observable.just(kv.key))
        .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
      delete(tx, keys);
    });
  }

  public void delete(Observable<byte[]> keys) {
    delete(lmdb.internalWriteTx(), keys);
  }

  public void delete(RxTx tx, Observable<byte[]> keys) {
    keys.subscribe(new DeleteSubscriber(this, tx));
  }

  public <T> Observable<List<T>> scan(Scan<T> scan) {
    return scan(defaultBuffer, lmdb.internalReadTx(), scan);
  }

  public <T> Observable<List<T>> scan(int buffer, Scan<T> scan) {
    return scan(buffer, lmdb.internalReadTx(), scan);
  }

  public Observable<List<KeyValue>> scan(KeyRange... ranges) {
    return scan(defaultBuffer, lmdb.internalReadTx(), scanDefault, ranges);
  }

  public Observable<List<KeyValue>> scan(int buffer, KeyRange... ranges) {
    return scan(buffer, lmdb.internalReadTx(), scanDefault, ranges);
  }

  public Observable<List<KeyValue>> scan(RxTx tx, KeyRange... ranges) {
    return scan(defaultBuffer, tx, scanDefault, ranges);
  }

  public Observable<List<KeyValue>> scan(int buffer, RxTx tx, KeyRange... ranges) {
    return scan(buffer, tx, scanDefault, ranges);
  }

  public <T> Observable<List<T>> scan(Scan<T> scan, KeyRange... ranges) {
    return scan(defaultBuffer, lmdb.internalReadTx(), scan, ranges);
  }

  public <T> Observable<List<T>> scan(int buffer, Scan<T> scan, KeyRange... ranges) {
    return scan(buffer, lmdb.internalReadTx(), scan, ranges);
  }

  public <T> Observable<List<T>> scan(RxTx tx, Scan<T> scan, KeyRange... ranges) {
    return scan(defaultBuffer, tx, scan, ranges);
  }

  public <T> Observable<List<T>> scan(int buffer, RxTx tx, Scan<T> scan, KeyRange... ranges) {
    return Scanners.scan(db, tx, scan, scheduler, buffer, ranges);
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
    RuntimeException ex;

    private PutSubscriber(RxDB db, RxTx tx) {
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
      tx.abort();
    }

    @Override
    public void onNext(KeyValue kv) {
      try {
        db.put(tx.tx, kv.key, kv.value);
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
