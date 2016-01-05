package org.deephacks.rxlmdb;

import io.reactivesocket.Payload;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

class KeyValuePayload implements Payload {
  private static final byte[] EMPTY = new byte[0];
  private final ByteBuffer data;
  private final ByteBuffer meta;
  private final MutableDirectBuffer dataBuf;
  private final MutableDirectBuffer metaBuf;

  public KeyValuePayload(KeyValue kv, OpType type) {
    this(kv.key, kv.value, type);
  }

  public KeyValuePayload(byte[] key, OpType type) {
    this(key, EMPTY, type);
  }

  public KeyValuePayload(byte[] key, byte[] value, OpType type) {
    this.dataBuf = Payloads.POOL.acquireMutableDirectBuffer(8 + key.length + value.length);
    int offset = 0;

    this.dataBuf.putInt(offset, key.length);
    offset += BitUtil.SIZE_OF_INT;

    this.dataBuf.putBytes(offset, key);
    offset += key.length;

    this.dataBuf.putInt(offset, value.length);
    offset += BitUtil.SIZE_OF_INT;

    this.dataBuf.putBytes(offset, value);

    this.metaBuf = Payloads.POOL.acquireMutableDirectBuffer(4);
    this.metaBuf.putInt(0, type.id);

    this.data = dataBuf.byteBuffer();
    this.meta = metaBuf.byteBuffer();
  }

  public static KeyValue getKeyValue(Payload payload) {
    ByteBuffer bb = payload.getData();
    if (bb.capacity() == 0) {
      return null;
    }
    UnsafeBuffer data = new UnsafeBuffer(bb);
    int offset = 0;
    int size = data.getInt(offset);
    offset += BitUtil.SIZE_OF_INT;

    byte[] key = new byte[size];
    data.getBytes(offset, key);
    offset += size;

    size = data.getInt(offset);
    offset += BitUtil.SIZE_OF_INT;

    byte[] value = new byte[size];
    data.getBytes(offset, value);
    return new KeyValue(key, value);
  }

  public static byte[] getByteArray(Payload payload) {
    ByteBuffer bb = payload.getData();
    if (bb.capacity() == 0) {
      return null;
    }
    UnsafeBuffer data = new UnsafeBuffer(bb);
    int size = data.getInt(0);
    byte[] key = new byte[size];
    data.getBytes(4, key);
    return key;
  }

  @Override
  public ByteBuffer getData() {
    return data;
  }

  @Override
  public ByteBuffer getMetadata() {
    return meta;
  }

  public void release() {
    Payloads.POOL.release(metaBuf);
    Payloads.POOL.release(dataBuf);
  }
}
