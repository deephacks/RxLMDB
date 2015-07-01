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
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;

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
    put(null, values);
  }

  public void put(RxTx tx, Observable<KeyValue> values) {
    values.subscribe(new PutSubscriber(this, tx));
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

  private <T> Observable<List<T>> scan(int buffer, RxTx tx, Scan<T> scan, KeyRange... ranges) {
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
    final boolean closeTx;

    private PutSubscriber(RxDB db, RxTx tx) {
      this.closeTx = tx != null ? false : true;
      this.tx = tx != null ? tx : db.lmdb.writeTx();
      this.db = db.db;
    }

    @Override
    public void onCompleted() {
      if (closeTx) {
        tx.commit();
      }
    }

    @Override
    public void onError(Throwable e) {
      e.printStackTrace();
      tx.abort();
    }

    @Override
    public void onNext(KeyValue kv) {
      db.put(tx.tx, kv.key, kv.value);
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

//
//  private static class OnScanSubscribe<T> implements Observable.OnSubscribe<T> {
//    private RxTx tx;
//    private boolean closeTx;
//    private final RxDB db;
//    private final KeyRange range;
//    private final Scan<T> scan;
//    private final boolean delete;
//
//    private OnScanSubscribe(RxDB db, RxTx tx, Scan<T> scan, KeyRange range, boolean delete) {
//      this.range = range;
//      this.db = db;
//      this.tx = tx;
//      this.scan = scan;
//      this.delete = delete;
//    }
//
//    @Override
//    public void call(Subscriber<? super T> subscriber) {
//      try {
//        this.closeTx = tx != null ? false : true;
//        this.tx = tx != null ? tx : (delete ? db.lmdb.writeTx() : db.lmdb.readTx());
//        BufferCursor cursor = db.db.bufferCursor(tx.tx);
//        DirectBuffer stop = range.stop != null ? new DirectBuffer(range.stop) : new DirectBuffer(new byte[0]);
//        boolean hasNext = range.start != null ? cursor.seek(range.start) :
//          (range.forward ? cursor.next() : cursor.prev());
//        while (hasNext) {
//          if (range.forward && range.stop != null && compareTo(cursor.keyBuffer(), stop) <= 0) {
//            T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
//            if (result != null) {
//              if (delete) {
//                db.db.delete(tx.tx, cursor.keyBuffer());
//              } else {
//                subscriber.onNext(result);
//              }
//            }
//          } else if (!range.forward && range.stop != null && compareTo(cursor.keyBuffer(), stop) >= 0) {
//            T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
//            if (result != null) {
//              if (delete) {
//                db.db.delete(tx.tx, cursor.keyBuffer());
//              } else {
//                subscriber.onNext(result);
//              }
//            }
//          } else if (range.stop == null) {
//            T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
//            if (result != null) {
//              if (delete) {
//                db.db.delete(tx.tx, cursor.keyBuffer());
//              } else {
//                subscriber.onNext(result);
//              }
//            }
//          } else {
//            break;
//          }
//          hasNext = range.forward ? cursor.next() : cursor.prev();
//        }
//      } catch (Throwable e) {
//        subscriber.onError(e);
//      } finally {
//        if (closeTx) {
//          tx.commit();
//        }
//      }
//      subscriber.onCompleted();
//    }
//  }
}
