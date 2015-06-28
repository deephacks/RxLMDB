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

/**
 * Ranges are inclusive and keys are prefix matched.
 */
public class KeyRange {
  public final byte[] start;
  public final byte[] stop;
  public final boolean forward;

  private KeyRange(byte[] start, byte[] stop, boolean forward) {
    this.start = start;
    this.stop = stop;
    this.forward = forward;
  }

  public static KeyRange forward() {
    return new KeyRange(null, null, true);
  }

  public static KeyRange backward() {
    return new KeyRange(null, null, false);
  }

  public static KeyRange atLeast(byte[] start) {
    return new KeyRange(start, null, true);
  }

  public static KeyRange atLeastBackward(byte[] start) {
    return new KeyRange(start, null, false);
  }

  public static KeyRange atMost(byte[] stop) {
    return new KeyRange(null, stop, true);
  }

  public static KeyRange atMostBackward(byte[] stop) {
    return new KeyRange(null, stop, false);
  }

  public static KeyRange range(byte[] start, byte[] stop) {
    if (DirectBufferComparator.compareTo(start, stop) <= 0) {
      return new KeyRange(start, stop, true);
    } else {
      return new KeyRange(start, stop, false);
    }
  }
}
