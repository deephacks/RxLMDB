package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.Entry;

import java.util.Arrays;

public class KeyValue {
  public final byte[] key;
  public final byte[] value;

  KeyValue(Entry entry) {
    this.key = entry.getKey();
    this.value = entry.getValue();
  }

  public KeyValue(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return "KeyValue{" + "key=" + Arrays.toString(key) +
      ", value=" + Arrays.toString(value) + '}';
  }
}
