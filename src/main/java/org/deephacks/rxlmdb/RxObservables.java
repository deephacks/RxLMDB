package org.deephacks.rxlmdb;

import rx.Observable;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class RxObservables {
  static <T> Stream<T> toStreamBlocking(Observable<T> observable) {
    return StreamSupport.stream(observable.toBlocking().toIterable().spliterator(), false);
  }
}
