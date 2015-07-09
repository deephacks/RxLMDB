package org.deephacks.rxlmdb;

import com.squareup.wire.Wire;
import generated.User;
import org.fusesource.lmdbjni.DirectBuffer;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class ProtoForwardRangeScan {

  static RangedRowsSetup setup = new RangedRowsSetup(ProtoForwardRangeScan.class);
  static Wire wire = new Wire();

  @State(Scope.Thread)
  public static class PlainThread extends AbstractPlainThread {
    public PlainThread() {
      super(setup, cursor -> parseFrom(cursor.valBuffer()).ssn.toByteArray());
    }
  }

  @State(Scope.Thread)
  public static class RxThread extends AbstractRxThread {
    public RxThread() {
      super(setup, (key, value) -> parseFrom(value).ssn.toByteArray());
    }
  }

  @Setup
  public void setup() {
    setup.writeProtoRanges();
  }

  @Benchmark
  public void rx(RxThread t) {
    t.next();
  }

  @Benchmark
  public void plain(PlainThread t) {
    t.next();
  }

  static final User parseFrom(DirectBuffer value) {
    try {
      byte[] bytes = new byte[value.capacity()];
      value.getBytes(0, bytes);
      return wire.parseFrom(bytes, User.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
