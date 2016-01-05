package org.deephacks.rxlmdb;

import org.slf4j.impl.SimpleLogger;
import rx.functions.Func1;
import uk.co.real_logic.aeron.driver.Configuration;

import java.util.concurrent.TimeUnit;

public interface Base {

  default Func1<KeyValue, Boolean> keyPrefix(String prefix) {
    return kv -> new String(kv.key).startsWith(prefix);
  }

  static void setDebugMode() {
    String hourNs = Long.toString(TimeUnit.HOURS.toNanos(1));
    System.setProperty("reactivesocket.aeron.tracingEnabled", "true");
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE");
    System.setProperty(Configuration.STATUS_MESSAGE_TIMEOUT_PROP_NAME, hourNs);
    System.setProperty(Configuration.CLIENT_LIVENESS_TIMEOUT_PROP_NAME, hourNs);
    System.setProperty(Configuration.IMAGE_LIVENESS_TIMEOUT_PROP_NAME, hourNs);
    System.setProperty(Configuration.PUBLICATION_LINGER_PROP_NAME, hourNs);
  }
}
