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
      Scanner<T> scanner = getScanner(db.bufferCursor(tx.tx), scan, KeyRange.forward());
      return createObservable(scanner, tx).buffer(buffer);
    } else if (ranges.length == 1) {
      Scanner<T> scanner = getScanner(db.bufferCursor(tx.tx), scan, ranges[0]);
      return createObservable(scanner, tx).buffer(buffer);
    }
    return Arrays.asList(ranges).stream()
      .map(range -> createObservable(getScanner(db.bufferCursor(tx.tx), scan, range), tx)
        .buffer(buffer).subscribeOn(scheduler).onBackpressureBuffer())
      .reduce(Observable.empty(), (o1, o2) -> o1.mergeWith(o2));
  }

  private static final <T> Scanner<T> getScanner(BufferCursor cursor, Scan<T> scan, KeyRange range) {
    switch (range.type) {
      case FORWARD:
        return new ForwardScan<>(cursor, scan, range);
      case FORWARD_START:
        return new ForwardStartScan<>(cursor, scan, range);
      case FORWARD_STOP:
        return new ForwardStopScan<>(cursor, scan, range);
      case FOWARD_RANGE:
        return new ForwardRangeScan<>(cursor, scan, range);
      case BACKWARD:
        return new BackwardScan<>(cursor, scan, range);
      case BACKWARD_START:
        return new BackwardStartScan<>(cursor, scan, range);
      case BACKWARD_STOP:
        return new BackwardStopScan<>(cursor, scan, range);
      case BACKWARD_RANGE:
        return new BackwardRangeScan<>(cursor, scan, range);
      default:
        return new ForwardScan<>(cursor, scan, range);
    }
  }

  private static final <T> Observable<T> createObservable(Scanner<T> scanner, RxTx tx) {
    return Observable.create(subscriber -> {
      try {
        scanner.execute(subscriber);
        subscriber.onCompleted();
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
    final BufferCursor cursor;
    final Scan<T> scan;
    final KeyRange range;

    protected Scanner(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      this.cursor = cursor;
      this.scan = scan;
      this.range = range;
    }

    static final <T> void onNext(Subscriber<? super T> subscriber, BufferCursor cursor, Scan<T> scan) {
      T result = scan.map(cursor.keyBuffer(), cursor.valBuffer());
      if (result != null) {
        subscriber.onNext(result);
      }
    }

    public abstract void execute(Subscriber<? super T> subscriber);
  }

  static class ForwardScan<T> extends Scanner<T> {

    protected ForwardScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      boolean hasNext = cursor.first();
      while (hasNext) {
        onNext(subscriber, cursor, scan);
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardScan<T> extends Scanner<T> {

    protected BackwardScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      boolean hasNext = cursor.last();
      while (hasNext) {
        onNext(subscriber, cursor, scan);
        hasNext = cursor.prev();
      }
    }
  }

  static class ForwardStopScan<T> extends Scanner<T> {

    protected ForwardStopScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.first();
      while (hasNext) {
        if (compareTo(cursor.keyBuffer(), stop) <= 0) {
          onNext(subscriber, cursor, scan);
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardStopScan<T> extends Scanner<T> {

    protected BackwardStopScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.last();
      while (hasNext) {
        if (compareTo(cursor.keyBuffer(), stop) >= 0) {
          onNext(subscriber, cursor, scan);
        }
        hasNext = cursor.prev();
      }
    }
  }

  static class ForwardStartScan<T> extends Scanner<T> {

    protected ForwardStartScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        onNext(subscriber, cursor, scan);
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardStartScan<T> extends Scanner<T> {

    protected BackwardStartScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        onNext(subscriber, cursor, scan);
        hasNext = cursor.prev();
      }
    }
  }


  static class ForwardRangeScan<T> extends Scanner<T> {

    protected ForwardRangeScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (compareTo(cursor.keyBuffer(), stop) <= 0) {
          onNext(subscriber, cursor, scan);
        }
        hasNext = cursor.next();
      }
    }
  }

  static class BackwardRangeScan<T> extends Scanner<T> {

    protected BackwardRangeScan(BufferCursor cursor, Scan<T> scan, KeyRange range) {
      super(cursor, scan, range);
    }

    @Override
    public void execute(Subscriber<? super T> subscriber) {
      DirectBuffer stop = new DirectBuffer(range.stop);
      boolean hasNext = cursor.seek(range.start);
      while (hasNext) {
        if (compareTo(cursor.keyBuffer(), stop) >= 0) {
          onNext(subscriber, cursor, scan);
        }
        hasNext = cursor.prev();
      }
    }
  }
}
