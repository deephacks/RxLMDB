package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;

public interface DirectMapper<T> {
  /**
   * Provide a zero copy key/value pair applied to a given result.
   * @return null will be skipped from result
   */
  T map(DirectBuffer key, DirectBuffer value);

  class KeyValueMapper implements DirectMapper<KeyValue> {

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