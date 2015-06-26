package org.deephacks.rxlmdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

class IoUtil {
  public static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator + "rxlmdb";

  public static Path createTmpDir() {
    try {
      Path path = Paths.get(TMP_DIR);
      Files.createDirectories(path);
      return Files.createTempDirectory(path, "");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Path createPathOrTemp(Optional<Path> optional) {
    Path path = optional.orElse(createTmpDir());
    try {
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return path;
  }
}
