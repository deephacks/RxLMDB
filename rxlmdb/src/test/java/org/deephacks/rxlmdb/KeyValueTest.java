package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.truth.Truth.assertThat;

public class KeyValueTest {

  @Test
  public void testFromDirectBuffer() {
    DirectBuffer buffer = new DirectBuffer(ByteBuffer.allocateDirect(1 + 2 + 4 + 8));
    buffer.putByte(0, (byte) 1);
    buffer.putShort(1, (short) 2, ByteOrder.BIG_ENDIAN);
    buffer.putInt(3, 3, ByteOrder.BIG_ENDIAN);
    buffer.putLong(7, 4, ByteOrder.BIG_ENDIAN);

    KeyValue kv = new KeyValue(buffer, buffer);
    assertThat(kv.getKeyByte(0)).isEqualTo((byte) 1);
    assertThat(kv.getKeyShort(1)).isEqualTo((short) 2);
    assertThat(kv.getKeyInt(3)).isEqualTo(3);
    assertThat(kv.getKeyLong(7)).isEqualTo(4L);
  }
}
