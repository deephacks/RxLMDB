package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Transaction;
import org.openjdk.jmh.annotations.*;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.deephacks.rxlmdb.DirectBufferComparator.compareTo;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class KeyValueForwardRangeScan {

  static RangedRowsSetup setup = new RangedRowsSetup(KeyValueForwardRangeScan.class);
  static AtomicInteger THREAD_ID = new AtomicInteger(0);
  static KeyRange[] ranges;

  @State(Scope.Thread)
  public static class RxThread {
    private final int id = THREAD_ID.getAndIncrement();
    private RxTx tx;
    private Iterator<KeyValue> values;
    private Iterator<List<KeyValue>> obs;

    public RxThread() {
      tx = setup.lmdb.readTx();
      obs = setup.db.scan(tx, ranges[id], ranges[id])
        .toBlocking().toIterable().iterator();
      values = obs.next().iterator();
    }

    public void next() {
      if (values.hasNext()) {
        values.next();
      } else if (obs.hasNext()) {
        values = obs.next().iterator();
      } else {
        obs = setup.db.scan(100_000, tx, ranges[id], ranges[id])
          .toBlocking().toIterable().iterator();
        values = obs.next().iterator();
      }
    }
  }

  @State(Scope.Thread)
  public static class PlainThread {
    private final int id = THREAD_ID.getAndIncrement();
    private BufferCursor cursor;
    private Transaction tx;
    private DirectBuffer stop;

    public PlainThread() {
      tx = setup.lmdb.env.createReadTransaction();
      cursor = setup.db.db.bufferCursor(tx);
      stop = new DirectBuffer(ranges[id].start);
    }

    public void next() {
      if (cursor.next() && compareTo(cursor.keyBuffer(), stop) <= 0) {
        new KeyValue(cursor.keyBytes(), cursor.valBytes());
      } else {
        cursor.seek(ranges[id].start);
      }
    }
  }

  @Setup
  public void setup() {
    ranges = setup.writeRanges();
  }

  @Benchmark
  public void rx(RxThread t) {
    t.next();
  }

  @Benchmark
  public void plain(PlainThread t) {
    t.next();
  }
}
