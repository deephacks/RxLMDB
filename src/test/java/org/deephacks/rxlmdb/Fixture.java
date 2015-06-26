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

  public static final List<KeyValue> oneToFive = Arrays.asList(
    new KeyValue(_1, _1),
    new KeyValue(_2, _2),
    new KeyValue(_3, _3),
    new KeyValue(_4, _4),
    new KeyValue(_5, _5)
  );

  public static final List<KeyValue> fiveToOne = Arrays.asList(
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
      result = oneToFive.stream()
        .filter(b -> FastKeyComparator.withinKeyRange(b.key, start, stop))
        .collect(Collectors.toCollection(LinkedList::new));

    } else {
      result = fiveToOne.stream()
        .filter(b -> FastKeyComparator.withinKeyRange(b.key, stop, start))
        .collect(Collectors.toCollection(LinkedList::new));
    }
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Range was empty");
    }
    return result;
  }

}
