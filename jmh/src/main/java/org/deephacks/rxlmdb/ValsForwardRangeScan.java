package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class ValsForwardRangeScan {

  static RangedRowsSetup setup = new RangedRowsSetup(ValsForwardRangeScan.class);

  @State(Scope.Thread)
  public static class PlainThread extends AbstractPlainThread {
    public PlainThread() {
      super(setup, cursor -> parseFrom(cursor.valBuffer()).getSsn());
    }
  }

  @State(Scope.Thread)
  public static class RxThread extends AbstractRxThread {
    public RxThread() {
      super(setup, (key, value) -> parseFrom(value).getSsn());
    }
  }

  @Setup
  public void setup() {
    setup.writeValsRanges();
  }

  @Benchmark
  public void rx(RxThread t) {
    t.next();
  }

  @Benchmark
  public void plain(PlainThread t) {
    t.next();
  }

  static final UserVal parseFrom(DirectBuffer value) {
    org.deephacks.vals.DirectBuffer buffer =
      new org.deephacks.vals.DirectBuffer(value.addressOffset(), value.capacity());
    return UserValBuilder.parseFrom(buffer);
  }
}
