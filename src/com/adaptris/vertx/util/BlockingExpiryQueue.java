package com.adaptris.vertx.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.util.TimeInterval;

/**
 * <p>
 * This queue works almost identically to an ArrayBlockingQueue, with one difference. <br/>
 * When you attempt to put(T) an item on the queue we will attempt to put this item at the tail of the queue, but if the queue is full we will block until the configured timeout.
 * Which when surpassed will remove the head of the queue, because it has now timed out and again attempt to append the new item to the tail of the queue.
 * </p>
 * <p>
 * You may also register an {@link ExpiryListener}, which will be triggered when we do have to expire and item.
 * </p>
 * 
 * @author Aaron
 *
 * @param <T>
 */
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
