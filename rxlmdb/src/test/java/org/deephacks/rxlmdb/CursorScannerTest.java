package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture.*;
import static org.deephacks.rxlmdb.RxObservables.toStreamBlocking;

public class CursorScannerTest {
  RxDb db;

  @Before
  public void before() {
    db = RxDb.tmp();
    db.put(Observable.from(_1_to_9));
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testScanSingle() {
    LinkedList<KeyValue> expected = Fixture.range(__1, __1);
    toStreamBlocking(db.cursor((cursor, subscriber) ->  {
      cursor.first();
      subscriber.onNext(cursor.keyBytes());
    })).forEach(key -> assertThat(expected.pollFirst().key()).isEqualTo(key));
    assertThat(expected).isEqualTo(new LinkedList<>());
  }

  @Test
  public void testScanMultiple() {
    List<Object> list = toStreamBlocking(db.cursor((cursor, subscriber) -> {
      cursor.first();
      subscriber.onNext(cursor.keyBytes());
      cursor.last();
      subscriber.onNext(cursor.keyBytes());
    })).collect(Collectors.toList());
    assertThat(list.size()).isEqualTo(2);
    assertThat((byte[]) list.get(0)).isEqualTo(__1);
    assertThat((byte[]) list.get(1)).isEqualTo(__9);
  }
}
