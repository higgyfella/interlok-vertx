package com.adaptris.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import io.vertx.core.VertxOptions;

public class VertxPropertyBuilderTest {
  
  private VertxPropertyBuilder builder;
  
  @Before
  public void setUp() throws Exception {
    builder = new VertxPropertyBuilder();
  }
  
  @Test
  public void testNullProperties() throws Exception {
    VertxOptions vertxOptions = builder.build(null);
    
    assertNotNull(vertxOptions);
  }
  
  @Test
  public void testAllValuesSet() throws Exception {
    VertxProperties vertxProperties = new VertxProperties();
    vertxProperties.setBlockedThreadCheckInterval(1L);
    vertxProperties.setEventLoopPoolSize(1);
    vertxProperties.setHaEnabled(true);
    vertxProperties.setHaGroup("my-ha-group");
    vertxProperties.setInternalBlockingPoolSize(1);
    vertxProperties.setMaxEventLoopExecuteTime(1L);
    vertxProperties.setMaxWorkerExecuteTime(1L);
    vertxProperties.setPreferNativeTransport(true);
    vertxProperties.setQuorumSize(1);
    vertxProperties.setWarningExceptionTime(1L);
    vertxProperties.setWorkerPoolSize(1);
    
    VertxOptions vertxOptions = builder.build(vertxProperties);
    
    assertEquals(1L, vertxOptions.getBlockedThreadCheckInterval());
    assertEquals(1, vertxOptions.getEventLoopPoolSize());
    assertEquals(true, vertxOptions.isHAEnabled());
    assertEquals("my-ha-group", vertxOptions.getHAGroup());
    assertEquals(1, vertxOptions.getInternalBlockingPoolSize());
    assertEquals(1L, vertxOptions.getMaxEventLoopExecuteTime());
    assertEquals(1L, vertxOptions.getMaxWorkerExecuteTime());
    assertEquals(true, vertxOptions.getPreferNativeTransport());
    assertEquals(1, vertxOptions.getQuorumSize());
    assertEquals(1L, vertxOptions.getWarningExceptionTime());
    assertEquals(1, vertxOptions.getWorkerPoolSize());
  }
  
  @Test
  public void testNoValuesSet() throws Exception {
    VertxOptions vertxOptions = builder.build(new VertxProperties());
    
    assertNotNull(vertxOptions);
  }

}
