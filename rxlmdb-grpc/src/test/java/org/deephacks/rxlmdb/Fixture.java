package org.deephacks.rxlmdb;

public class Fixture {

  public static KeyValue kv(String prefix, int i) {
    byte[] key = String.format("%skey%03d", prefix, i).getBytes();
    byte[] val = String.format("%sval%03d", prefix, i).getBytes();
    return new KeyValue(key, val);
  }


}
