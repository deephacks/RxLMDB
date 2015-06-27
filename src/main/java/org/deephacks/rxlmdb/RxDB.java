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

  private static class OnScanSubscribe<R> implements Observable.OnSubscribe<KeyValue> {
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
        EntryIterator it;
        if (range.forward) {
          it = range.start != null ? db.db.seek(tx.tx, range.start) : db.db.iterate(tx.tx);
        } else {
          it = range.start != null ? db.db.seekBackward(tx.tx, range.start) : db.db.iterateBackward(tx.tx);
        }
        while (it.hasNext()) {
          Entry next = it.next();
          if (range.forward && range.stop != null && withinKeyRange(next.getKey(), range.stop)) {
            subscriber.onNext(new KeyValue(next));
          } else if (!range.forward && range.stop != null && withinKeyRange(range.stop, next.getKey())) {
            subscriber.onNext(new KeyValue(next));
          } else if (range.stop == null) {
            subscriber.onNext(new KeyValue(next));
          } else {
            break;
          }
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
