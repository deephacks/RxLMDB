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
    List<KeyValue> result = RxObservables.toSingleStreamBlocking(db.get(Observable.just(__1)))
      .collect(Collectors.toList());
    assertThat(result).hasSize(1);
    assertThat(result.get(0).key).isEqualTo(__1);
  }

  @Test
  public void testGetNull() {
    List<KeyValue> result = RxObservables.toSingleStreamBlocking(
      db.get(Observable.from(new byte[][]{__1, null, __2}))
    ).collect(Collectors.toList());
    assertThat(result).hasSize(3);
    assertThat(result.get(0).key).isEqualTo(__1);
    assertThat(result.get(0).value).isEqualTo(__1);
    assertThat(result.get(1).key).isEqualTo(null);
    assertThat(result.get(1).value).isEqualTo(null);
    assertThat(result.get(2).key).isEqualTo(__2);
    assertThat(result.get(2).value).isEqualTo(__2);
  }


  @Test
  public void testGetMultiple() {
    List<KeyValue> result = RxObservables.toSingleStreamBlocking(
      db.get(Observable.from(new byte[][]{__3, __1, __2}))
    ).collect(Collectors.toList());
    assertThat(result).hasSize(3);
    assertThat(result.get(0).key).isEqualTo(__3);
    assertThat(result.get(1).key).isEqualTo(__1);
    assertThat(result.get(2).key).isEqualTo(__2);

  }

  @Test
  public void testGetNonExisting() {
    List<KeyValue> result = RxObservables.toSingleStreamBlocking(db.get(Observable.just(new byte[]{123})))
      .collect(Collectors.toList());
    assertThat(result).hasSize(1);
    assertThat(result.get(0).key).isEqualTo(new byte[] { 123 });
    assertThat(result.get(0).value).isNull();
  }

  @Test
  public void testGetSomeExisting() {
    List<KeyValue> result = RxObservables.toSingleStreamBlocking(
      db.get(Observable.from(new byte[][]{ new byte[]{ 123 }, __9, new byte[]{ 123 }, __8, }))
    ).collect(Collectors.toList());
    assertThat(result).hasSize(4);
    assertThat(result.get(0).key).isEqualTo(new byte[]{123});
    assertThat(result.get(0).value).isNull();
    assertThat(result.get(1).key).isEqualTo(__9);
    assertThat(result.get(1).value).isEqualTo(__9);
    assertThat(result.get(2).key).isEqualTo(new byte[]{123});
    assertThat(result.get(2).value).isNull();
    assertThat(result.get(3).key).isEqualTo(__8);
    assertThat(result.get(3).value).isEqualTo(__8);
  }
}
