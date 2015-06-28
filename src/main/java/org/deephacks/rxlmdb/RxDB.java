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

import com.google.common.primitives.UnsignedBytes;
import org.fusesource.lmdbjni.*;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;

import java.util.Arrays;
import java.util.Optional;

import static org.deephacks.rxlmdb.DirectBufferComparator.compareTo;

public class RxDB {
  final Scan.ScanDefault scanDefault = new Scan.ScanDefault();
  final RxLMDB lmdb;
  final Database db;
  final String name;
  final Scheduler scheduler;

  private RxDB(Builder builder) {
    this.lmdb = builder.lmdb;
    this.name = Optional.ofNullable(builder.name).orElse("default");
    this.db = lmdb.env.openDatabase(this.name);
    this.scheduler = lmdb.scheduler;
  }

  public <T> Observable<T> scan(Scan<T> scan) {
    return scan(null, scan);
  }

  public Observable<KeyValue> scan(KeyRange... ranges) {
    return scan(null, scanDefault, ranges);
  }

  public Observable<KeyValue> scan(RxTx tx, KeyRange... ranges) {
    return scan(tx, scanDefault, ranges);
  }

  public <T> Observable<T> scan(Scan<T> scan, KeyRange... ranges) {
    return scan(null, scan, ranges);
  }

  public <T> Observable<T> scan(RxTx tx, Scan<T> scan, KeyRange... ranges) {
    if (ranges.length == 0) {
      return Observable.create(new OnScanSubscribe(this, tx, scan, KeyRange.forward()));
    } else if (ranges.length == 1) {
      return Observable.create(new OnScanSubscribe(this, tx, scan, ranges[0]));
    }
    return Arrays.asList(ranges).stream()
      .map(range -> Observable.create(new OnScanSubscribe(this, tx, scan, range))
        .subscribeOn(scheduler))
      .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
  }

  public void delete(Observable<byte[]> keys) {
    delete(null, keys);
  }

  public void delete(RxTx tx, Observable<byte[]> keys) {
    keys.subscribe(new DeleteSubscriber(this, tx));
  }

  public void put(Observable<KeyValue> values) {
    put(null, values);
  }

  public void put(RxTx tx, Observable<KeyValue> values) {
    values.subscribe(new PutSubscriber(this, tx));
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
    final boolean closeTx;

    private DeleteSubscriber(RxDB db, RxTx tx) {
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
      tx.abort();
    }

    @Override
    public void onNext(byte[] key) {
      db.delete(tx.tx, key);
    }
  }

  private static class OnScanSubscribe<T> implements Observable.OnSubscribe<T> {
    private RxTx tx;
    private boolean closeTx;
    private final RxDB db;
    private final KeyRange range;
    private final Scan<T> scan;

    private OnScanSubscribe(RxDB db, RxTx tx, Scan<T> scan, KeyRange range) {
      this.range = range;
      this.db = db;
      this.tx = tx;
      this.scan = scan;
    }

    @Override
    public void call(Subscriber<? super T> subscriber) {
      try {
        this.closeTx = tx != null ? false : true;
        this.tx = tx != null ? tx : db.lmdb.readTx();
        BufferCursor cursor = db.db.bufferCursor(tx.tx);
        DirectBuffer stop = range.stop != null ? new DirectBuffer(range.stop) : new DirectBuffer(new byte[0]);
        boolean hasNext = range.start != null ? cursor.seek(range.start) :
          (range.forward ? cursor.next() : cursor.prev());
        while (hasNext) {
          if (range.forward && range.stop != null && compareTo(cursor.keyBuffer(), stop) <= 0) {
            T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
            if (result != null) {
              subscriber.onNext(result);
            }
          } else if (!range.forward && range.stop != null && compareTo(cursor.keyBuffer(), stop) >= 0) {
            T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
            if (result != null) {
              subscriber.onNext(result);
            }
          } else if (range.stop == null) {
            T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
            if (result != null) {
              subscriber.onNext(result);
            }
          } else {
            break;
          }
          hasNext = range.forward ? cursor.next() : cursor.prev();
        }
      } catch (Throwable e) {
        subscriber.onError(e);
      } finally {
        if (closeTx) {
          tx.abort();
        }
      }
      subscriber.onCompleted();
    }
  }
}
