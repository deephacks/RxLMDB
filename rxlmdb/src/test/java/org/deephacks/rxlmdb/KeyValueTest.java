package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.truth.Truth.assertThat;

public class KeyValueTest {

  @Test
  public void testFromDirectBuffer() {
    DirectBuffer buffer = new DirectBuffer(ByteBuffer.allocateDirect(1 + 4 + 8));
    buffer.putByte(0, (byte) 1);
    buffer.putInt(1, 2, ByteOrder.BIG_ENDIAN);
    buffer.putLong(5, 3, ByteOrder.BIG_ENDIAN);

    KeyValue kv = new KeyValue(buffer, buffer);
    assertThat(kv.getKeyByte(0)).isEqualTo((byte) 1);
    assertThat(kv.getKeyInt(1)).isEqualTo(2);
    assertThat(kv.getKeyLong(5)).isEqualTo(3L);
  }
}
