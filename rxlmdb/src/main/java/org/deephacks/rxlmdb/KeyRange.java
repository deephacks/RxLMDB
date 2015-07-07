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

import static org.deephacks.rxlmdb.KeyRange.KeyRangeType.*;

/**
 * Ranges are inclusive and keys are prefix matched.
 */
public class KeyRange {
  public final byte[] start;
  public final byte[] stop;
  public final KeyRangeType type;

  private KeyRange(byte[] start, byte[] stop, KeyRangeType type) {
    this.start = start;
    this.stop = stop;
    this.type = type;
  }

  public static KeyRange forward() {
    return new KeyRange(null, null, FORWARD);
  }

  public static KeyRange backward() {
    return new KeyRange(null, null, BACKWARD);
  }

  public static KeyRange atLeast(byte[] start) {
    return new KeyRange(start, null, FORWARD_START);
  }

  public static KeyRange atLeastBackward(byte[] start) {
    return new KeyRange(start, null, BACKWARD_START);
  }

  public static KeyRange atMost(byte[] stop) {
    return new KeyRange(null, stop, FORWARD_STOP);
  }

  public static KeyRange atMostBackward(byte[] stop) {
    return new KeyRange(null, stop, BACKWARD_STOP);
  }

  public static KeyRange range(byte[] start, byte[] stop) {
    if (DirectBufferComparator.compareTo(start, stop) <= 0) {
      return new KeyRange(start, stop, FOWARD_RANGE);
    } else {
      return new KeyRange(start, stop, BACKWARD_RANGE);
    }
  }

  enum KeyRangeType {
    FORWARD, FORWARD_START, FORWARD_STOP, FOWARD_RANGE, BACKWARD, BACKWARD_START, BACKWARD_STOP, BACKWARD_RANGE
  }
}
