package com.adaptris.vertx;

import java.util.concurrent.TimeUnit;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.VertxOptions;

/**
 * <p>
 * This class exposes many of the internal Vertx engine properties.
 * </p>
 * <p>
 * You may specify your own values for each or simply leave Vertx to create sensible defaults.
 * </p>
 * 
 * @config vertx-properties
 * @since 3.10.0
 */
@AdapterComponent
@ComponentProfile(summary = "Exposes many of the Vertx engine options.", tag = "clustering,vertx")
@XStreamAlias("vertx-properties")
public class VertxProperties {

  private Integer eventLoopPoolSize;
  private Integer workerPoolSize;
  private Integer internalBlockingPoolSize;
  private Long blockedThreadCheckInterval;
  private Long maxEventLoopExecuteTime;
  private Long maxWorkerExecuteTime;
  private Boolean haEnabled;
  private Integer quorumSize;
  private String haGroup;
  private Long warningExceptionTime;
  private Boolean preferNativeTransport;
  
  
  public Integer getEventLoopPoolSize() {
    return eventLoopPoolSize;
  }
  /**
   * Set the number of event loop threads to be used by the Vert.x instance.
   *
   * @param eventLoopPoolSize the number of threads
   */
  public void setEventLoopPoolSize(Integer eventLoopPoolSize) {
    this.eventLoopPoolSize = eventLoopPoolSize;
  }
  public Integer getWorkerPoolSize() {
    return workerPoolSize;
  }
  /**
   * Set the maximum number of worker threads to be used by the Vert.x instance.
   *
   * @param workerPoolSize the number of threads
   */
  public void setWorkerPoolSize(Integer workerPoolSize) {
    this.workerPoolSize = workerPoolSize;
  }
  public Integer getInternalBlockingPoolSize() {
    return internalBlockingPoolSize;
  }
  /**
   * Set the value of internal blocking pool size
   *
   * @param internalBlockingPoolSize the maximumn number of threads in the internal blocking pool
   */
  public void setInternalBlockingPoolSize(Integer internalBlockingPoolSize) {
    this.internalBlockingPoolSize = internalBlockingPoolSize;
  }
  public Long getBlockedThreadCheckInterval() {
    return blockedThreadCheckInterval;
  }
  /**
   * Sets the value of blocked thread check period, in {@link VertxOptions#setBlockedThreadCheckIntervalUnit blockedThreadCheckIntervalUnit}.
   * <p>
   * The default value of {@link VertxOptions#setBlockedThreadCheckIntervalUnit blockedThreadCheckIntervalUnit} is {@link TimeUnit#MILLISECONDS}
   *
   * @param blockedThreadCheckInterval the value of blocked thread check period, in {@link VertxOptions#setBlockedThreadCheckIntervalUnit blockedThreadCheckIntervalUnit}.
   */
  public void setBlockedThreadCheckInterval(Long blockedThreadCheckInterval) {
    this.blockedThreadCheckInterval = blockedThreadCheckInterval;
  }
  public Long getMaxEventLoopExecuteTime() {
    return maxEventLoopExecuteTime;
  }
  /**
   * Sets the value of max event loop execute time, in {@link VertxOptions#setMaxEventLoopExecuteTimeUnit maxEventLoopExecuteTimeUnit}.
   * <p>
   * The default value of {@link VertxOptions#setMaxEventLoopExecuteTimeUnit maxEventLoopExecuteTimeUnit}is {@link TimeUnit#NANOSECONDS}
   *
   * @param maxEventLoopExecuteTime the value of max event loop execute time, in {@link VertxOptions#setMaxEventLoopExecuteTimeUnit maxEventLoopExecuteTimeUnit}.
   */
  public void setMaxEventLoopExecuteTime(Long maxEventLoopExecuteTime) {
    this.maxEventLoopExecuteTime = maxEventLoopExecuteTime;
  }
  public Long getMaxWorkerExecuteTime() {
    return maxWorkerExecuteTime;
  }
  /**
   * Sets the value of max worker execute time, in {@link VertxOptions#setMaxWorkerExecuteTimeUnit maxWorkerExecuteTimeUnit}.
   * <p>
   * The default value of {@link VertxOptions#setMaxWorkerExecuteTimeUnit maxWorkerExecuteTimeUnit} is {@link TimeUnit#NANOSECONDS}
   *
   * @param maxWorkerExecuteTime the value of max worker execute time, in {@link VertxOptions#setMaxWorkerExecuteTimeUnit maxWorkerExecuteTimeUnit}.
   */
  public void setMaxWorkerExecuteTime(Long maxWorkerExecuteTime) {
    this.maxWorkerExecuteTime = maxWorkerExecuteTime;
  }
  public Boolean getHaEnabled() {
    return haEnabled;
  }
  /**
   * Set whether HA will be enabled on the Vert.x instance.
   *
   * @param haEnabled true if enabled, false if not.
   */
  public void setHaEnabled(Boolean haEnabled) {
    this.haEnabled = haEnabled;
  }
  public Integer getQuorumSize() {
    return quorumSize;
  }
  /**
   * Set the quorum size to be used when HA is enabled.
   *
   * @param quorumSize the quorum size
   */
  public void setQuorumSize(Integer quorumSize) {
    this.quorumSize = quorumSize;
  }
  public String getHaGroup() {
    return haGroup;
  }
  /**
   * Set the HA group to be used when HA is enabled.
   *
   * @param haGroup the HA group to use
   */
  public void setHaGroup(String haGroup) {
    this.haGroup = haGroup;
  }
  public Long getWarningExceptionTime() {
    return warningExceptionTime;
  }
  /**
   * Set the threshold value above this, the blocked warning contains a stack trace. in {@link VertxOptions#setWarningExceptionTimeUnit warningExceptionTimeUnit}.
   * The default value of {@link VertxOptions#setWarningExceptionTimeUnit warningExceptionTimeUnit} is {@link TimeUnit#NANOSECONDS}
   *
   * @param warningExceptionTime
   */
  public void setWarningExceptionTime(Long warningExceptionTime) {
    this.warningExceptionTime = warningExceptionTime;
  }
  public Boolean getPreferNativeTransport() {
    return preferNativeTransport;
  }
  /**
   * Set wether to prefer the native transport to the JDK transport.
   *
   * @param preferNativeTransport {@code true} to prefer the native transport
   */
  public void setPreferNativeTransport(Boolean preferNativeTransport) {
    this.preferNativeTransport = preferNativeTransport;
  }
  
  
}
