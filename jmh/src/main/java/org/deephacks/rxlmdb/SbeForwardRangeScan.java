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
public class SbeForwardRangeScan {

  static RangedRowsSetup setup = new RangedRowsSetup(SbeForwardRangeScan.class);

  @State(Scope.Thread)
  public static class PlainThread extends AbstractPlainThread {
    public PlainThread() {
      super(setup, cursor -> parseFrom(cursor.valBuffer()));
    }
  }

  @State(Scope.Thread)
  public static class RxThread extends AbstractRxThread {
    public RxThread() {
      super(setup, (key, value) -> parseFrom(value));
    }
  }

  @Setup
  public void setup() {
    setup.writeSbeRanges();
  }

  @Benchmark
  public void rx(RxThread t) {
    t.next();
  }

  @Benchmark
  public void plain(PlainThread t) {
    t.next();
  }

  static final generated.sbe.User parseFrom(DirectBuffer value) {
    uk.co.real_logic.sbe.codec.java.DirectBuffer buffer =
      new uk.co.real_logic.sbe.codec.java.DirectBuffer(value.addressOffset(), value.capacity());
    generated.sbe.User user = new generated.sbe.User();
    user.wrapForDecode(buffer, 0, 0, 0);
    byte[] bytes = new byte[16];
    user.getEmail(bytes, 0, 16);
    return user;
  }
}
