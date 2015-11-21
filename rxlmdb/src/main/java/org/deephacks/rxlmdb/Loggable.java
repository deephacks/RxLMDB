package org.deephacks.rxlmdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Loggable {

    default void info(String message, Object... args) {
        logger().info(message, args);
    }

    default void error(String message, Throwable t) {
        logger().error(message, t);
    }

    default void debug(String message, Object... args) {
        logger().debug(message, args);
    }

    default void trace(String message, Object... args) {
        logger().trace(message, args);
    }

    default boolean isTraceEnabled() {
        if (Const.TRACING_ENABLED) {
            return logger().isTraceEnabled();
        } else {
            return false;
        }
    }

    default Logger logger() {
        return LoggerFactory.getLogger(getClass());
    }
}