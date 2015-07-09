package org.deephacks.rxlmdb;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class ZeroCopyForwardRangeScanBig {
  static RangedRowsSetup setup = new RangedRowsSetup(KeyValueForwardRangeScanBig.class);

  @State(Scope.Thread)
  public static class PlainThread extends AbstractPlainThread {
    public PlainThread() {
      super(setup, cursor -> cursor.keyByte(0));
    }
  }

  @Setup
  public void setup() {
    setup.writeRangesBig();
  }

  @Benchmark
  public void plain(PlainThread t) {
    t.next();
  }

}
