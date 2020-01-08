package com.adaptris.vertx.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.adaptris.util.TimeInterval;

public class BlockingExpiryQueueTest {
  
  private BlockingExpiryQueue<String> blockingExpiryQueue;
  
  @Mock
  private ExpiryListener<String> mockExpiryListener;
  
  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    blockingExpiryQueue = new BlockingExpiryQueue<>(3);
    blockingExpiryQueue.registerExpiryListener(mockExpiryListener);
    blockingExpiryQueue.setExpiryTimeout(new TimeInterval(100L, TimeUnit.MILLISECONDS));
  }
  
  @After
  public void tearDown() throws Exception {
    
  }
  
  @Test
  public void testPutAndRetrieve() throws Exception {
    blockingExpiryQueue.put("1");
    blockingExpiryQueue.put("2");
    blockingExpiryQueue.put("3");
    
    assertEquals("1", blockingExpiryQueue.take());
    assertEquals("2", blockingExpiryQueue.take());
    assertEquals("3", blockingExpiryQueue.take());
    
    assertNull(blockingExpiryQueue.poll(1L, TimeUnit.SECONDS));
    
    verify(mockExpiryListener, never()).itemExpired("1");
  }
  
  @Test
  public void testExpireHead() throws Exception {
    blockingExpiryQueue.put("1");
    blockingExpiryQueue.put("2");
    blockingExpiryQueue.put("3");
    blockingExpiryQueue.put("4"); // this will expire the first item
    
    assertEquals("2", blockingExpiryQueue.take());
    assertEquals("3", blockingExpiryQueue.take());
    assertEquals("4", blockingExpiryQueue.take());
    
    verify(mockExpiryListener, times(1)).itemExpired("1");
  }

}
