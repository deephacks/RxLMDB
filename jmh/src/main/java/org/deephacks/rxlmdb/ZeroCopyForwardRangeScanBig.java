package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Transaction;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.deephacks.rxlmdb.DirectBufferComparator.compareTo;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class ZeroCopyForwardRangeScanBig {

  static RangedRowsSetup setup = new RangedRowsSetup(KeyValueForwardRangeScanBig.class);
  static AtomicInteger THREAD_ID = new AtomicInteger(0);
  static KeyRange[] ranges;

  @State(Scope.Thread)
  public static class PlainThread {
    private final int id = THREAD_ID.getAndIncrement();
    private BufferCursor cursor;
    private Transaction tx;
    private DirectBuffer stop;

    public PlainThread() {
      tx = setup.lmdb.env.createReadTransaction();
      cursor = setup.db.db.bufferCursor(tx);
      stop = new DirectBuffer(ranges[id].stop);
      cursor.seek(ranges[id].start);
    }

    public void next() {
      if (cursor.next() && compareTo(cursor.keyBuffer(), stop) <= 0) {
        cursor.keyByte(0);
      } else {
        cursor.seek(ranges[id].start);
      }
    }
  }

  @Setup
  public void setup() {
    ranges = setup.writeRangesBig();
  }

  @Benchmark
  public void plain(PlainThread t) {
    t.next();
  }

}
