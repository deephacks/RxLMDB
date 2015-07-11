package org.deephacks.rxlmdb;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class KeyValueForwardRangeScan {

  static RangedRowsSetup setup = new RangedRowsSetup(KeyValueForwardRangeScan.class);

  @State(Scope.Thread)
  public static class RxThread extends AbstractRxThread {
    public RxThread() {
      super(setup, new Scan.ScanDefault());
    }
  }

  @State(Scope.Thread)
  public static class PlainThread extends AbstractPlainThread {
    public PlainThread() {
      super(setup, cursor -> new KeyValue(cursor.keyBytes(), cursor.valBytes()));
    }
  }

  @Setup
  public void setup() {
    setup.writeSmallKeyValue();
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
