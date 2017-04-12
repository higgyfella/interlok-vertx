package com.adaptris.vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * The mode in which to send messages.
 * 
 */
public class SendMode {
  public enum Mode {
    /**
     * Use {@code SINGLE} instead.
     * 
     */
    @Deprecated
    single,
    /**
     * Use {@code ALL} instead.
     * 
     */
    @Deprecated
    all,
    ALL,
    SINGLE
  }

  private static boolean warningLogged = false;
  private static final Logger log = LoggerFactory.getLogger(SendMode.class);

  static boolean single(Mode mode) {
    logDeprecated(mode);
    return mode == Mode.single || mode == Mode.SINGLE;
  }

  private static void logDeprecated(Mode m) {
    if (!warningLogged && (m == Mode.single || m == Mode.all)) {
      log.warn("{} is deprecated, use {} instead", m.name(), m == Mode.single ? Mode.SINGLE.name() : Mode.ALL.name());
      warningLogged = true;
    }
  }
}
