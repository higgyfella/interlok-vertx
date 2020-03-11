package com.adaptris.vertx;

import io.vertx.core.VertxOptions;

public class VertxPropertyBuilder {
  
  private VertxOptions vertxOptions;
  
  public VertxPropertyBuilder() {
    this.setVertxOptions(new VertxOptions());
  }
  
  public VertxOptions build(VertxProperties properties) {
    if(properties == null)
      return this.getVertxOptions();
    
    if(properties.getHaEnabled() != null)  this.getVertxOptions().setHAEnabled(properties.getHaEnabled());
    if(properties.getPreferNativeTransport() != null)  this.getVertxOptions().setPreferNativeTransport(properties.getPreferNativeTransport());
    if(properties.getBlockedThreadCheckInterval() != null)  this.getVertxOptions().setBlockedThreadCheckInterval(properties.getBlockedThreadCheckInterval());
    if(properties.getEventLoopPoolSize() != null)  this.getVertxOptions().setEventLoopPoolSize(properties.getEventLoopPoolSize());
    if(properties.getHaGroup() != null)  this.getVertxOptions().setHAGroup(properties.getHaGroup());
    if(properties.getInternalBlockingPoolSize() != null)  this.getVertxOptions().setInternalBlockingPoolSize(properties.getInternalBlockingPoolSize());
    if(properties.getMaxEventLoopExecuteTime() != null)  this.getVertxOptions().setMaxEventLoopExecuteTime(properties.getMaxEventLoopExecuteTime());
    if(properties.getMaxWorkerExecuteTime() != null)  this.getVertxOptions().setMaxWorkerExecuteTime(properties.getMaxWorkerExecuteTime());
    if(properties.getQuorumSize() != null)  this.getVertxOptions().setQuorumSize(properties.getQuorumSize());
    if(properties.getWarningExceptionTime() != null)  this.getVertxOptions().setWarningExceptionTime(properties.getWarningExceptionTime());
    if(properties.getWorkerPoolSize() != null)  this.getVertxOptions().setWorkerPoolSize(properties.getWorkerPoolSize());
    
    return this.getVertxOptions();
  }

  public VertxOptions getVertxOptions() {
    return vertxOptions;
  }

  public void setVertxOptions(VertxOptions vertxOptions) {
    this.vertxOptions = vertxOptions;
  }

}
