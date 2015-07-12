package org.deephacks.rxlmdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.UUID;

import static com.google.common.truth.Truth.assertThat;

public class EnvTest {
  RxDB db;

  @Before
  public void before() {
    db = RxDB.tmp();
  }

  @After
  public void after() {
    db.close();
    db.lmdb.close();
  }

  @Test
  public void testTmpPath() {
    Path path = db.lmdb.getPath();
    assertThat(path.toString()).startsWith(IoUtil.TMP_DIR);
  }

  @Test
  public void testSize() {
    long size = db.lmdb.getSize();
    assertThat(size).isGreaterThan(64_000_000L);
    assertThat(size).isLessThan(128_000_000L);
  }

  @Test
  public void testPath() {
    String tmp = IoUtil.TMP_DIR + UUID.randomUUID().toString();
    RxLMDB lmdb = RxLMDB.builder().path(tmp).build();
    Path path = lmdb.getPath();
    assertThat(path.toString()).isEqualTo(tmp);
  }

  @Test
  public void tesAllEnv() {
    String tmp = IoUtil.TMP_DIR + UUID.randomUUID().toString();
    RxLMDB.builder()
      .path(tmp)
      .fixedmap()
      .mapAsync()
      .noLock()
      .noMemInit()
      .noMetaSync()
      .noReadahead()
      .noSync()
      .build();
  }
}
