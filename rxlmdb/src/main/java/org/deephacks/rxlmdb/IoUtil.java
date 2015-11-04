/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.rxlmdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

class IoUtil {
  static final String TMP_DIR = System.getProperty("java.io.tmpdir") + File.separator + "rxlmdb";

  static Path createTmpDir() {
    try {
      Path path = Paths.get(TMP_DIR);
      Files.createDirectories(path);
      return Files.createTempDirectory(path, "");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static Path createPathOrTemp(Path path) {
    Path aPath = Optional.ofNullable(path).orElse(createTmpDir());
    try {
      Files.createDirectories(aPath);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
    return aPath;
  }
}
