package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture._1_to_9;
import static org.deephacks.rxlmdb.Fixture.*;

public class GetTest {
  RxDB db;

  @Before
  public void before() {
    db = RxDB.tmp();
    db.put(Observable.from(_1_to_9));
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testGetSingle() {
    List<byte[]> result = RxObservables.toSingleStreamBlocking(db.get(Observable.just(__1)))
      .collect(Collectors.toList());
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(__1);
  }

  @Test
  public void testGetMultiple() {
    List<byte[]> result = RxObservables.toSingleStreamBlocking(
      db.get(Observable.from(new byte[][]{__3, __1, __2}))
    ).collect(Collectors.toList());
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo(__3);
    assertThat(result.get(1)).isEqualTo(__1);
    assertThat(result.get(2)).isEqualTo(__2);

  }

  @Test
  public void testGetNonExisting() {
    List<byte[]> result = RxObservables.toSingleStreamBlocking(db.get(Observable.just(new byte[]{123})))
      .collect(Collectors.toList());
    assertThat(result).hasSize(0);
  }

  @Test
  public void testGetSomeExisting() {
    List<byte[]> result = RxObservables.toSingleStreamBlocking(
      db.get(Observable.from(new byte[][]{ new byte[]{ 123 }, __9, new byte[]{ 123 }, __8, }))
    ).collect(Collectors.toList());
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).isEqualTo(__9);
    assertThat(result.get(1)).isEqualTo(__8);
  }
}
