package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.deephacks.rxlmdb.Fixture._1_to_9;
import static org.deephacks.rxlmdb.Fixture.__1;

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
  public void testGet() {
    List<byte[]> result = RxObservables.toSingleStreamBlocking(db.get(__1))
      .collect(Collectors.toList());
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(__1);
  }

  @Test
  public void testGetNonExisting() {
    List<byte[]> result = RxObservables.toSingleStreamBlocking(db.get(new byte[]{123}))
      .collect(Collectors.toList());
    assertThat(result).hasSize(0);
  }
}
