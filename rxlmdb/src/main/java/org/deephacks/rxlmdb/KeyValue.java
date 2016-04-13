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
  private final byte[] key;
  private final byte[] value;

  private final DirectBuffer keyBuffer;
  private final DirectBuffer valueBuffer;

  KeyValue(Entry entry) {
    if (entry == null) {
      throw new NullPointerException();
    }
    if (entry.getKey() == null) {
      throw new NullPointerException();
    }
    if (entry.getValue() == null) {
      throw new NullPointerException();
    }
    this.key = entry.getKey();
    this.value = entry.getValue();
    this.keyBuffer = null;
    this.valueBuffer = null;
  }

  public KeyValue(byte[] key, byte[] value) {
    if (key == null) {
      throw new NullPointerException();
    }
    if (value == null) {
      throw new NullPointerException();
    }
    this.key = key;
    this.value = value;
    this.valueBuffer = null;
    this.keyBuffer = null;
  }

  public KeyValue(DirectBuffer key, DirectBuffer value) {
    if (key == null) {
      throw new NullPointerException();
    }
    if (value == null) {
      throw new NullPointerException();
    }
    this.keyBuffer = key;
    this.valueBuffer = value;
    this.key = null;
    this.value = null;
  }

  public byte[] key() {
    return key;
  }

  public byte[] value() {
    return value;
  }

  public DirectBuffer keyBuffer() {
    return keyBuffer;
  }

  public DirectBuffer valueBuffer() {
    return valueBuffer;
  }
}
