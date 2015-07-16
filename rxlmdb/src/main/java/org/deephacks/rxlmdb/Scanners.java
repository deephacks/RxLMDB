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

class Scanners {

  static final <T> Observable<List<T>> scan(Database db,
                                                   RxTx tx,
                                                   DirectMapper<T> mapper,
                                                   Scheduler scheduler,
                                                   int buffer,
                                                   KeyRange... ranges) {
    if (ranges.length == 0) {
      Scanner<T> scanner = getScanner(db, tx, mapper, KeyRange.forward());
      return createObservable(scanner, tx).buffer(buffer);
    } else if (ranges.length == 1) {
      Scanner<T> scanner = getScanner(db, tx, mapper, ranges[0]);
      return createObservable(scanner, tx).buffer(buffer);
    }
    if (!tx.isUserManaged) {
      throw new IllegalArgumentException("Parallel scan transactions must be handled by the user");
    }
    return Arrays.asList(ranges).stream()
      .map(range -> createObservable(getScanner(db, tx, mapper, range), tx)
        .buffer(buffer).subscribeOn(scheduler).onBackpressureBuffer())
      .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
  }

  static <T> Observable<List<T>> scan(Database db, int buffer, RxTx tx, CursorScanner<T> scanner) {
    return createObservable(new CursorScan(db, tx, null, null, scanner), tx).buffer(buffer);
  }


  private static final <T> Scanner<T> getScanner(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
    switch (range.type) {
      case FORWARD:
        return new ForwardScan<>(db, tx, mapper, range);
      case FORWARD_START:
        return new ForwardStartScan<>(db, tx, mapper, range);
      case FORWARD_STOP:
        return new ForwardStopScan<>(db, tx, mapper, range);
      case FOWARD_RANGE:
        return new ForwardRangeScan<>(db, tx, mapper, range);
      case BACKWARD:
        return new BackwardScan<>(db, tx, mapper, range);
      case BACKWARD_START:
        return new BackwardStartScan<>(db, tx, mapper, range);
      case BACKWARD_STOP:
        return new BackwardStopScan<>(db, tx, mapper, range);
      case BACKWARD_RANGE:
        return new BackwardRangeScan<>(db, tx, mapper, range);
      default:
        return new ForwardScan<>(db, tx, mapper, range);
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
    final DirectMapper<T> mapper;
    final KeyRange range;
    BufferCursor cursor;

    protected Scanner(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      this.db = db;
      this.tx = tx;
      this.mapper = mapper;
      this.range = range;
    }

    public abstract void execute(Subscriber<? super T> subscriber);
  }

  static class ForwardScan<T> extends Scanner<T> {

    protected ForwardScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.first();
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardScan<T> extends Scanner<T> {

    protected BackwardScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.last();
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.prev();
      }
    }
  }

  static class ForwardStopScan<T> extends Scanner<T> {

    protected ForwardStopScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
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
          T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
          if (result != null) {
            subscriber.onNext(result);
          }
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardStopScan<T> extends Scanner<T> {

    protected BackwardStopScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
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
          T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
          if (result != null) {
            subscriber.onNext(result);
          }
        }
        hasNext = cursor.prev();
      }
    }
  }

  static class ForwardStartScan<T> extends Scanner<T> {

    protected ForwardStartScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardStartScan<T> extends Scanner<T> {

    protected BackwardStartScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (subscriber.isUnsubscribed()) {
          return;
        }
        T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
        hasNext = cursor.prev();
      }
    }
  }


  static class ForwardRangeScan<T> extends Scanner<T> {

    protected ForwardRangeScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      BufferCursor cursor = db.bufferCursor(tx.tx);
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.seek(range.start);
      if (!hasNext) {
        return;
      } else {
        T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
      }
      while (!subscriber.isUnsubscribed() && cursor.next() && compareTo(cursor.keyBuffer(), stop) <= 0) {
        T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
        if (result != null) {
          subscriber.onNext(result);
        }
      }
    }
  }

  static class BackwardRangeScan<T> extends Scanner<T> {

    protected BackwardRangeScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range) {
      super(db, tx, mapper, range);
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
          T result = mapper.map(cursor.keyBuffer(), cursor.valBuffer());
          if (result != null) {
            subscriber.onNext(result);
          }
        }
        hasNext = cursor.prev();
      }
    }
  }
  static class CursorScan<T> extends Scanner<T> {
    private final CursorScanner<T> scanner;

    protected CursorScan(Database db, RxTx tx, DirectMapper<T> mapper, KeyRange range, CursorScanner<T> scanner) {
      super(db, tx, mapper, range);
      this.scanner = scanner;
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      try (BufferCursor cursor = db.bufferCursor(tx.tx)) {
        scanner.execute(cursor, subscriber);
      }
    }
  }
}
