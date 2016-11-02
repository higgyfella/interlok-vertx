package com.adaptris.vertx.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.util.TimeInterval;

public class BlockingExpiryQueue<T> extends ArrayBlockingQueue<T> {
  
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());
  
  private static long DEFAULT_EXPIRY_TIMEOUT_SECONDS = 30;
  
  private static final long serialVersionUID = -835483665919502942L;
  
  private transient List<ExpiryListener<T>> expiryListeners;
  
  private TimeInterval expiryTimeout;

  public BlockingExpiryQueue(int capacity) {
    this(capacity, true);
  }
  
  public BlockingExpiryQueue(int capacity, boolean fairPolicy) {
    super(capacity, fairPolicy);
    expiryListeners = new ArrayList<>();
    this.setExpiryTimeout(new TimeInterval(DEFAULT_EXPIRY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
  }

  public void put(T object) throws InterruptedException {
    while(!this.offer(object, this.getExpiryTimeout().toMilliseconds(), TimeUnit.MILLISECONDS)) {
      T taken = this.take();
      log.trace("Queue full, attempting to remove the expired item at the head of the queue: " + taken);
      this.notifyListenerOfExpiry(taken);
      log.trace("Removed expired head of the queue: " + taken);
    }
      
  } 
  
  public void registerExpiryListener(ExpiryListener<T> listener) {
    expiryListeners.add(listener);
  }
  
  public void notifyListenerOfExpiry(T object) {
    for(ExpiryListener<T> listener : this.expiryListeners)
      listener.itemExpired(object);
  }

  public TimeInterval getExpiryTimeout() {
    return expiryTimeout;
  }

  public void setExpiryTimeout(TimeInterval expiryTimeout) {
    this.expiryTimeout = expiryTimeout;
  }
}
