package com.adaptris.vertx;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.adaptris.core.CoreException;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.TimeInterval;

class ConsumerLatch {

  private static final long DEFAULT_TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES).toMilliseconds();

  private CountDownLatch barrier;

  private ConsumerLatch() {
    barrier = new CountDownLatch(1);
  }

  static ConsumerLatch build() {
    return new ConsumerLatch();
  }

  void waitForComplete() throws CoreException {
    try {
      barrier.await(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  void complete() {
    barrier.countDown();
  }

}
