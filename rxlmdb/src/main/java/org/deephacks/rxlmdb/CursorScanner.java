package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import rx.Subscriber;

public interface CursorScanner<T> {
  void execute(BufferCursor cursor, Subscriber<? super T> subscriber);
}
