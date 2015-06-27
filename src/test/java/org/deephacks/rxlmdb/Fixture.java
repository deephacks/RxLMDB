package org.deephacks.rxlmdb;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class Fixture {
  public static final byte[] _1 = new byte[] { 1 };
  public static final byte[] _2 = new byte[] { 2 };
  public static final byte[] _3 = new byte[] { 3 };
  public static final byte[] _4 = new byte[] { 4 };
  public static final byte[] _5 = new byte[] { 5 };
  public static final byte[] _6 = new byte[] { 6 };
  public static final byte[] _7 = new byte[] { 7 };
  public static final byte[] _8 = new byte[] { 8 };
  public static final byte[] _9 = new byte[] { 9 };

  public static final KeyValue[] values = new KeyValue[] {
    new KeyValue(_1, _1),
    new KeyValue(_2, _2),
    new KeyValue(_3, _3),
    new KeyValue(_4, _4),
    new KeyValue(_5, _5),
    new KeyValue(_6, _6),
    new KeyValue(_7, _7),
    new KeyValue(_8, _8),
    new KeyValue(_9, _9)
  };

  public static final LinkedList<KeyValue> _1_to_9 = Stream.of(values)
    .collect(Collectors.toCollection(LinkedList::new));

  public static final LinkedList<KeyValue> _9_to_1 = Streams.reverse(_1_to_9);

  /**
   * inclusive ranges
   */
  public static LinkedList<KeyValue> range(byte[] start, byte[] stop) {
    LinkedList<KeyValue> result;
    if (new FastKeyComparator().compare(start, stop) < 0) {
      result = _1_to_9.stream()
        .filter(b -> FastKeyComparator.withinKeyRange(b.key, start, stop))
        .collect(Collectors.toCollection(LinkedList::new));

    } else {
      result = _9_to_1.stream()
        .filter(b -> FastKeyComparator.withinKeyRange(b.key, stop, start))
        .collect(Collectors.toCollection(LinkedList::new));
    }
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Range was empty");
    }
    return result;
  }

}
