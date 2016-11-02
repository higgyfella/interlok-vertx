package com.adaptris.vertx.util;

import java.util.concurrent.TimeUnit;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.verify;

import com.adaptris.util.TimeInterval;

import junit.framework.TestCase;

public class BlockingExpiryQueueTest extends TestCase {
  
  private BlockingExpiryQueue<String> blockingExpiryQueue;
  
  @Mock
  private ExpiryListener<String> mockExpiryListener;
  
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    blockingExpiryQueue = new BlockingExpiryQueue<>(3);
    blockingExpiryQueue.registerExpiryListener(mockExpiryListener);
    blockingExpiryQueue.setExpiryTimeout(new TimeInterval(100L, TimeUnit.MILLISECONDS));
  }
  
  public void tearDown() throws Exception {
    
  }
  
  public void testPutAndRetrieve() throws Exception {
    blockingExpiryQueue.put("1");
    blockingExpiryQueue.put("2");
    blockingExpiryQueue.put("3");
    
    assertEquals("1", blockingExpiryQueue.take());
    assertEquals("2", blockingExpiryQueue.take());
    assertEquals("3", blockingExpiryQueue.take());
    
    assertNull(blockingExpiryQueue.poll(1L, TimeUnit.SECONDS));
  }
  
  public void testExpireHead() throws Exception {
    blockingExpiryQueue.put("1");
    blockingExpiryQueue.put("2");
    blockingExpiryQueue.put("3");
    blockingExpiryQueue.put("4"); // this will expire the first item
    
    assertEquals("2", blockingExpiryQueue.take());
    assertEquals("3", blockingExpiryQueue.take());
    assertEquals("4", blockingExpiryQueue.take());
    
    verify(mockExpiryListener).itemExpired("1");
  }

}
