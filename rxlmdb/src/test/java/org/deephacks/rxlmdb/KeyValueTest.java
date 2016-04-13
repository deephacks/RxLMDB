package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.truth.Truth.assertThat;

public class KeyValueTest {

  @Test
  public void testFromDirectBuffer() {
    ByteOrder order = ByteOrder.BIG_ENDIAN;
    DirectBuffer buffer = new DirectBuffer(ByteBuffer.allocateDirect(1 + 2 + 4 + 8));
    buffer.putByte(0, (byte) 1);
    buffer.putShort(1, (short) 2, order);
    buffer.putInt(3, 3, order);
    buffer.putLong(7, 4, order);

    KeyValue kv = new KeyValue(buffer, buffer);
    DirectBuffer k = kv.keyBuffer();
    DirectBuffer v = kv.valueBuffer();

    assertThat(k.getByte(0)).isEqualTo((byte) 1);
    assertThat(v.getByte(0)).isEqualTo((byte) 1);

    assertThat(k.getShort(1, order)).isEqualTo((short) 2);
    assertThat(v.getShort(1, order)).isEqualTo((short) 2);

    assertThat(k.getInt(3, order)).isEqualTo(3);
    assertThat(v.getInt(3, order)).isEqualTo(3);

    assertThat(k.getLong(7, order)).isEqualTo(4L);
    assertThat(v.getLong(7, order)).isEqualTo(4L);
  }
}
