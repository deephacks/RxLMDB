package org.deephacks.rxlmdb;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Fixture {
  /** number of underscores signifies repeated number of b */

  public static final byte[] __1 = new byte[] { 1, 1 };
  public static final byte[] __2 = new byte[] { 2, 2 };
  public static final byte[] __3 = new byte[] { 3, 3 };
  public static final byte[] __4 = new byte[] { 4, 4 };
  public static final byte[] __5 = new byte[] { 5, 5 };
  public static final byte[] __6 = new byte[] { 6, 6 };
  public static final byte[] __7 = new byte[] { 7, 7 };
  public static final byte[] __8 = new byte[] { 8, 8 };
  public static final byte[] __9 = new byte[] { 9, 9 };

  public static final byte[] _1 = new byte[] { 1 };
  public static final byte[] _2 = new byte[] { 2 };
  public static final byte[] _3 = new byte[] { 3 };
  public static final byte[] _4 = new byte[] { 4 };
  public static final byte[] _5 = new byte[] { 5 };
  public static final byte[] _6 = new byte[] { 6 };
  public static final byte[] _7 = new byte[] { 7 };
  public static final byte[] _8 = new byte[] { 8 };
  public static final byte[] _9 = new byte[] { 9 };

  public static final byte[][] keys = new byte[][] { __1, __2, __3, __4, __5, __6, __7, __8, __9};

  public static final KeyValue[] values = new KeyValue[] {
    new KeyValue(__1, __1),
    new KeyValue(__2, __2),
    new KeyValue(__3, __3),
    new KeyValue(__4, __4),
    new KeyValue(__5, __5),
    new KeyValue(__6, __6),
    new KeyValue(__7, __7),
    new KeyValue(__8, __8),
    new KeyValue(__9, __9)
  };

  public static final LinkedList<KeyValue> _1_to_9 = Stream.of(values)
    .collect(Collectors.toCollection(LinkedList::new));

  public static final LinkedList<KeyValue> _9_to_1 = Streams.reverse(_1_to_9);

  /**
   * inclusive ranges
   */
  public static LinkedList<KeyValue> range(byte[] start, byte[] stop) {
    LinkedList<KeyValue> result;
    if (DirectBufferComparator.compareTo(start, stop) < 0) {
      result = _1_to_9.stream()
        .filter(b -> DirectBufferComparator.within(b.key, start, stop))
        .collect(Collectors.toCollection(LinkedList::new));

    } else {
      result = _9_to_1.stream()
        .filter(b -> DirectBufferComparator.within(b.key, stop, start))
        .collect(Collectors.toCollection(LinkedList::new));
    }
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Range was empty");
    }
    return result;
  }

}
