package org.deephacks.rxlmdb;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;

public class FastKeyComparator implements Comparator<byte[]> {
  public static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
  static final Unsafe theUnsafe;
  static final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  /** The offset to the first element in a byte array. */
  static final int BYTE_ARRAY_BASE_OFFSET;

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(
      (PrivilegedAction<Object>) () -> {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (NoSuchFieldException e) {
          // It doesn't matter what we throw;
          // it's swallowed in getBestComparer().
          throw new Error();
        } catch (IllegalAccessException e) {
          throw new Error();
        }
      }
    );

    BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

    // sanity check - this should never fail
    if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
      throw new AssertionError();
    }
  }

  @Override
  public int compare(byte[] o1, byte[] o2) {
    return compareTo(o1, 0, o1.length, o2, 0, o2.length);
  }

  public static boolean equals(byte[] o1, byte[] o2) {
    return compareTo(o1, 0, o1.length, o2, 0, o2.length) == 0;
  }

  public static boolean withinKeyRange(byte[] key, byte[] lastKey) {
    if (compareTo(lastKey, 0, lastKey.length, key, 0, key.length) < 0) {
      return false;
    }
    return true;
  }

  public static boolean withinKeyRange(byte[] key, byte[] firstKey, byte[] lastKey) {
    if (compareTo(firstKey, 0, firstKey.length, key, 0, key.length) > 0) {
      return false;
    }
    if (compareTo(lastKey, 0, lastKey.length, key, 0, key.length) < 0) {
      return false;
    }
    return true;
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
  public static int compareTo(byte[] buffer1, int offset1, int length1,
                              byte[] buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2 &&
      offset1 == offset2 &&
      length1 == length2) {
      return 0;
    }
    int minLength = Math.min(length1, length2);
    int minWords = minLength / LONG_BYTES;
    int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
    int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
     * time is no slower than comparing 4 bytes at a time even on 32-bit.
     * On the other hand, it is substantially faster on 64-bit.
     */
    for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
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
        buffer1[offset1 + i],
        buffer2[offset2 + i]);
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