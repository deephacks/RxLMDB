package org.deephacks.rxlmdb;

public class KeyRange {
  public final byte[] start;
  public final byte[] stop;
  public final boolean forward;

  private KeyRange(byte[] start, byte[] stop, boolean forward) {
    this.start = start;
    this.stop = stop;
    this.forward = forward;
  }

  public static KeyRange forward() {
    return new KeyRange(null, null, true);
  }

  public static KeyRange backward() {
    return new KeyRange(null, null, false);
  }

  public static KeyRange atLeast(byte[] start) {
    return new KeyRange(start, null, true);
  }

  public static KeyRange atLeastBackward(byte[] start) {
    return new KeyRange(start, null, false);
  }

  public static KeyRange atMost(byte[] stop) {
    return new KeyRange(null, stop, true);
  }

  public static KeyRange atMostBackward(byte[] stop) {
    return new KeyRange(null, stop, false);
  }

  public static KeyRange range(byte[] start, byte[] stop) {
    return new KeyRange(start, stop, true);
  }

  public static KeyRange rangeBackward(byte[] start, byte[] stop) {
    return new KeyRange(start, stop, false);
  }
}
