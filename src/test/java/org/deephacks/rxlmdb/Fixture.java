package org.deephacks.rxlmdb;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;


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

  public static final List<KeyValue> oneToNine = Arrays.asList(
    new KeyValue(_1, _1),
    new KeyValue(_2, _2),
    new KeyValue(_3, _3),
    new KeyValue(_4, _4),
    new KeyValue(_5, _5),
    new KeyValue(_6, _6),
    new KeyValue(_7, _7),
    new KeyValue(_8, _8),
    new KeyValue(_9, _9)
  );

  public static final List<KeyValue> nineToOne = Arrays.asList(
    new KeyValue(_9, _9),
    new KeyValue(_8, _8),
    new KeyValue(_7, _7),
    new KeyValue(_6, _6),
    new KeyValue(_5, _5),
    new KeyValue(_4, _4),
    new KeyValue(_3, _3),
    new KeyValue(_2, _2),
    new KeyValue(_1, _1)
  );

  /**
   * inclusive ranges
   */
  public static LinkedList<KeyValue> range(byte[] start, byte[] stop) {
    LinkedList<KeyValue> result;
    if (new FastKeyComparator().compare(start, stop) < 0) {
      result = oneToNine.stream()
        .filter(b -> FastKeyComparator.withinKeyRange(b.key, start, stop))
        .collect(Collectors.toCollection(LinkedList::new));

    } else {
      result = nineToOne.stream()
        .filter(b -> FastKeyComparator.withinKeyRange(b.key, stop, start))
        .collect(Collectors.toCollection(LinkedList::new));
    }
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Range was empty");
    }
    return result;
  }

}
