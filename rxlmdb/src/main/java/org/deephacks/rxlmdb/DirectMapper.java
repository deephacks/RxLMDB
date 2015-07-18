package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;

/**
 * Allow for zero-copy using addresses provided by LMDB instead of
 * copying data for each operation. Buffer addresses are valid for the
 * duration of a transaction. Accessing buffers outside a transaction
 * will cause SIGSEGV.
 *
 * @param <T> The mapped type.
 */
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
