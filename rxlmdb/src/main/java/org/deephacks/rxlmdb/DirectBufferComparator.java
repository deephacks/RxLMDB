package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.DirectBuffer;

import java.nio.ByteOrder;
import java.util.Comparator;

class DirectBufferComparator implements Comparator<DirectBuffer> {
  public static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  static final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  @Override
  public int compare(DirectBuffer o1, DirectBuffer o2) {
    return compareTo(o1, 0, o1.capacity(), o2, 0, o2.capacity());
  }

  public static Comparator<byte[]> byteArrayComparator() {
    return (o1, o2) -> compareTo(o1, o2);
  }

  public static int compareTo(byte[] key, byte[] stop) {
    return compareTo(new DirectBuffer(key), new DirectBuffer(stop));
  }

  public static boolean within(byte[] key, byte[] start, byte[] stop) {
    DirectBuffer startBuffer = new DirectBuffer(start);
    DirectBuffer stopBuffer = new DirectBuffer(stop);
    DirectBuffer keyBuffer = new DirectBuffer(key);
    if (compareTo(startBuffer, 0, stop.length, keyBuffer, 0, key.length) > 0) {
      return false;
    }
    if (compareTo(stopBuffer, 0, stop.length, keyBuffer, 0, key.length) < 0) {
      return false;
    }
    return true;
  }

  public static int compareTo(DirectBuffer key, DirectBuffer stop) {
    if (stop.capacity() > key.capacity()) {
      return -1;
    }
    return compareTo(key, 0, stop.capacity(), stop, 0, stop.capacity());
  }

  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to beginTransaction comparing in the left buffer
   * @param offset2 Where to beginTransaction comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, less 0 if left is less than right, etc.
   */
  public static int compareTo(DirectBuffer buffer1, int offset1, int length1,
                              DirectBuffer buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2 &&
      offset1 == offset2 &&
      length1 == length2) {
      return 0;
    }
    int minLength = Math.min(length1, length2);
    int minWords = minLength / LONG_BYTES;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
     * time is no slower than comparing 4 bytes at a time even on 32-bit.
     * On the other hand, it is substantially faster on 64-bit.
     */
    for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
      long lw = buffer1.getLong(i, ByteOrder.BIG_ENDIAN);
      long rw = buffer2.getLong(i, ByteOrder.BIG_ENDIAN);
      long diff = lw ^ rw;

      if (diff != 0) {
        if (!littleEndian) {
          return lessThanUnsigned(lw, rw) ? -1 : 1;
        }

        // Use binary search
        int n = 0;
        int y;
        int x = (int) diff;
        if (x == 0) {
          x = (int) (diff >>> 32);
          n = 32;
        }

        y = x << 16;
        if (y == 0) {
          n += 16;
        } else {
          x = y;
        }

        y = x << 8;
        if (y == 0) {
          n += 8;
        }
        return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
      }
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * LONG_BYTES; i < minLength; i++) {
      int result = compare(
        buffer1.getByte(offset1 + i),
        buffer2.getByte(offset2 + i));
      if (result != 0) {
        return result;
      }
    }
    return length1 - length2;
  }

  public static int compare(byte a, byte b) {
    return toInt(a) - toInt(b);
  }

  /**
   * Returns true if x1 is less than x2, when both values are treated as
   * unsigned.
   */
  static boolean lessThanUnsigned(long x1, long x2) {
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  public static int toInt(byte value) {
    return value & 0xFF;
  }
}