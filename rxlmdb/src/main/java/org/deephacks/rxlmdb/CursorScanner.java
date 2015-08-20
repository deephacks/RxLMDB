package org.deephacks.rxlmdb;

import org.fusesource.lmdbjni.BufferCursor;
import rx.Subscriber;

/**
 * Provide a BufferCursor for flexible cursor navigation during scans.
 */
public interface CursorScanner<T> {
  void execute(BufferCursor cursor, Subscriber<? super T> subscriber);
}
