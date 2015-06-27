package org.deephacks.rxlmdb;


import java.util.LinkedList;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class Streams {

  public static <T> LinkedList<T> reverse(LinkedList<T> list) {
    return StreamSupport.stream(
      Spliterators.spliteratorUnknownSize(list.descendingIterator(), Spliterator.ORDERED), false)
      .collect(Collectors.toCollection(LinkedList::new));
  }
}
