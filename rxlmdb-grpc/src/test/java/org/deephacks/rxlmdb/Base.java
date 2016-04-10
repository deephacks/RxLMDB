package org.deephacks.rxlmdb;

import org.slf4j.impl.SimpleLogger;
import rx.functions.Func1;

public interface Base {

  default Func1<KeyValue, Boolean> keyPrefix(String prefix) {
    return kv -> new String(kv.key).startsWith(prefix);
  }

  static void setDebugMode() {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
  }
}
