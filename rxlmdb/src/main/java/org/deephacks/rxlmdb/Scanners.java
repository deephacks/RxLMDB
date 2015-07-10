package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.DirectBuffer;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;

import java.util.Arrays;
import java.util.List;

import static org.deephacks.rxlmdb.DirectBufferComparator.compareTo;

public class Scanners {

  public static final <T> Observable<List<T>> scan(Database db,
                                                   RxTx tx,
                                                   Scan<T> scan,
                                                   Scheduler scheduler,
                                                   int buffer,
                                                   KeyRange... ranges) {
    if (ranges.length == 0) {
      Scanner<T> scanner = getScanner(db, tx, scan, KeyRange.forward());
      return createObservable(scanner, tx).buffer(buffer);
    } else if (ranges.length == 1) {
      Scanner<T> scanner = getScanner(db, tx, scan, ranges[0]);
      return createObservable(scanner, tx).buffer(buffer);
    }
    if (!tx.isUserManaged) {
      throw new IllegalArgumentException("Parallel scan transactions must be handled by the user");
    }
    return Arrays.asList(ranges).stream()
      .map(range -> createObservable(getScanner(db, tx, scan, range), tx)
        .buffer(buffer).subscribeOn(scheduler).onBackpressureBuffer())
      .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
  }

  private static final <T> Scanner<T> getScanner(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
    switch (range.type) {
      case FORWARD:
        return new ForwardScan<>(db, tx, scan, range);
      case FORWARD_START:
        return new ForwardStartScan<>(db, tx, scan, range);
      case FORWARD_STOP:
        return new ForwardStopScan<>(db, tx, scan, range);
      case FOWARD_RANGE:
        return new ForwardRangeScan<>(db, tx, scan, range);
      case BACKWARD:
        return new BackwardScan<>(db, tx, scan, range);
      case BACKWARD_START:
        return new BackwardStartScan<>(db, tx, scan, range);
      case BACKWARD_STOP:
        return new BackwardStopScan<>(db, tx, scan, range);
      case BACKWARD_RANGE:
        return new BackwardRangeScan<>(db, tx, scan, range);
      default:
        return new ForwardScan<>(db, tx, scan, range);
    }
  }

  private static final <T> Observable<T> createObservable(Scanner<T> scanner, RxTx tx) {
    return Observable.create(subscriber -> {
      try {
        scanner.execute(subscriber);
        if (!subscriber.isUnsubscribed()) {
          subscriber.onCompleted();
        }
      } catch (Throwable e) {
        if (!tx.isUserManaged) {
          tx.abort();
        }
        subscriber.onError(e);
      } finally {
        if (!tx.isUserManaged) {
          // no op if tx was aborted
          tx.commit();
        }
        scanner.cursor.close();
      }
    });
  }

  static abstract class Scanner<T> {
    final Database db;
    final RxTx tx;
    final Scan<T> scan;
    final KeyRange range;
    BufferCursor cursor;

    protected Scanner(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      this.db = db;
      this.tx = tx;
      this.scan = scan;
      this.range = range;
    }

    public abstract void execute(Subscriber<? super T> subscriber);
  }

  static class ForwardScan<T> extends Scanner<T> {

    protected ForwardScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.first();
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardScan<T> extends Scanner<T> {

    protected BackwardScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.last();
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.prev();
      }
    }
  }

  static class ForwardStopScan<T> extends Scanner<T> {

    protected ForwardStopScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.first();
      while (hasNext) {
        if (compareTo(cursor.keyBuffer(), stop) <= 0) {
          if (subscriber.isUnsubscribed()) {
            return;
          }
          T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
          if (result != null) {
            subscriber.onNext(result);
          }
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardStopScan<T> extends Scanner<T> {

    protected BackwardStopScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.last();
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        if (compareTo(cursor.keyBuffer(), stop) >= 0) {
          T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
          if (result != null) {
            subscriber.onNext(result);
          }
        }
        hasNext = cursor.prev();
      }
    }
  }

  static class ForwardStartScan<T> extends Scanner<T> {

    protected ForwardStartScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardStartScan<T> extends Scanner<T> {

    protected BackwardStartScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.prev();
      }
    }
  }


  static class ForwardRangeScan<T> extends Scanner<T> {

    protected ForwardRangeScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.seek(range.start);
      if (!hasNext) {
        return;
      } else {
        T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
      }
      while (!subscriber.isUnsubscribed() && cursor.next() && compareTo(cursor.keyBuffer(), stop) <= 0) {
        T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
      }
    }
  }

  static class BackwardRangeScan<T> extends Scanner<T> {

    protected BackwardRangeScan(Database db, RxTx tx, Scan<T> scan, KeyRange range) {
      super(db, tx, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        } else if (compareTo(cursor.keyBuffer(), stop) >= 0) {
          T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
          if (result != null) {
            subscriber.onNext(result);
          }
        }
        hasNext = cursor.prev();
      }
    }
  }
}
