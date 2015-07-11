package org.deephacks.rxlmdb;

import org.openjdk.jmh.annotations.*;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Measurement(iterations = 5)
@Warmup(iterations = 10)
@Fork(value = 2)
public class KeyValueForwardSkipRangeScan {
  static RangedRowsSetup setup = new RangedRowsSetup(KeyValueForwardSkipRangeScan.class);
  static KeyValue kv = new KeyValue(new byte[0], new byte[0]);
  @State(Scope.Thread)
  public static class RxThread extends AbstractRxThread {
    public RxThread() {
      super(setup, (key, value) -> {
        if ((key.getInt(1, ByteOrder.BIG_ENDIAN) & 5) == 0) {
          return new KeyValue(key, value);
        }
        return kv;
      });
    }
  }

  @State(Scope.Thread)
  public static class PlainThread extends AbstractPlainThread {
    public PlainThread() {
      super(setup, cursor -> {
        if ((cursor.keyBuffer().getInt(1, ByteOrder.BIG_ENDIAN) & 5) == 0) {
          new KeyValue(cursor.keyBytes(), cursor.valBytes());
        }
      });
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
