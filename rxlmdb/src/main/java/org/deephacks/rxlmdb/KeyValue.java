/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;
import org.fusesource.lmdbjni.Entry;

import java.nio.ByteOrder;
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

  public KeyValue(DirectBuffer key, DirectBuffer value) {
    byte[] k = new byte[key.capacity()];
    key.getBytes(0, k);
    byte[] v = new byte[value.capacity()];
    value.getBytes(0, v);
    this.key = k;
    this.value = v;
  }

  public byte getKeyByte(int pos) {
    return key[pos];
  }

  public short getKeyShort(int pos) {
    return new DirectBuffer(key).getShort(pos, ByteOrder.BIG_ENDIAN);
  }

  public int getKeyInt(int pos) {
    return new DirectBuffer(key).getInt(pos, ByteOrder.BIG_ENDIAN);
  }

  public long getKeyLong(int pos) {
    return new DirectBuffer(key).getLong(pos, ByteOrder.BIG_ENDIAN);
  }

  @Override
  public String toString() {
    return "KeyValue{" + "key=" + Arrays.toString(key) +
      ", value=" + Arrays.toString(value) + '}';
  }
}
