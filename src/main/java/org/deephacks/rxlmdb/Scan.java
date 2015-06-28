package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;

public interface Scan<T> {
  /**
   * Provide a zero copy key/value pair applied to a given result.
   * @return null will be omitted
   */
  T map(DirectBuffer key, DirectBuffer value);

  class ScanDefault implements Scan<KeyValue> {

    @Override
    public KeyValue map(DirectBuffer key, DirectBuffer value) {
      byte[] k = new byte[key.capacity()];
      key.getBytes(0, k);
      byte[] v = new byte[value.capacity()];
      value.getBytes(0, v);
      return new KeyValue(k, v);
    }
  }
}
