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
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;

import java.util.Arrays;
import java.util.Optional;

import static org.deephacks.rxlmdb.FastKeyComparator.withinKeyRange;

public class RxDB {
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

  public Observable<KeyValue> scan(KeyRange... ranges) {
    return scan(null, ranges);
  }

  public Observable<KeyValue> scan(RxTx tx, KeyRange... ranges) {
    if (ranges.length == 0) {
      return Observable.empty();
    } else if (ranges.length == 1) {
      return Observable.create(new OnScanSubscribe(this, tx, ranges[0]));
    }
    return Arrays.asList(ranges).stream()
      .map(range -> Observable.create(new OnScanSubscribe(this, tx, range))
        .subscribeOn(scheduler))
      .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
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

  private static class OnScanSubscribe implements Observable.OnSubscribe<KeyValue> {
    private RxTx tx;
    private boolean closeTx;
    private final RxDB db;
    private final KeyRange range;

    private OnScanSubscribe(RxDB db, RxTx tx, KeyRange range) {
      this.range = range;
      this.db = db;
      this.tx = tx;
    }

    @Override
    public void call(Subscriber<? super KeyValue> subscriber) {
      try {
        this.closeTx = tx != null ? false : true;
        this.tx = tx != null ? tx : db.lmdb.readTx();
        BufferCursor cursor = db.db.bufferCursor(tx.tx);
        boolean hasNext = range.start != null ? cursor.seek(range.start) :
          (range.forward ? cursor.next() : cursor.prev());
        while (hasNext) {
          byte[] key = cursor.keyBytes();
          if (range.forward && range.stop != null && withinKeyRange(key, range.stop)) {
            subscriber.onNext(new KeyValue(key, cursor.valBytes()));
          } else if (!range.forward && range.stop != null && withinKeyRange(range.stop, cursor.keyBytes())) {
            subscriber.onNext(new KeyValue(key, cursor.valBytes()));
          } else if (range.stop == null) {
            subscriber.onNext(new KeyValue(key, cursor.valBytes()));
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
