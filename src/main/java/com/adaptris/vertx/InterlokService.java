package com.adaptris.vertx;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("clustered-interlok-service")
public class InterlokService {
  
  private String id;
  private ServiceState state;
  private Exception exception;
  
  public InterlokService() {
    // no arg for marshalling
  }
  
  public InterlokService(String id) {
    this(id, ServiceState.NOT_STARTED);
  }
  
  public InterlokService(String id, ServiceState state) {
    this.setId(id);
    this.setState(state);
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public ServiceState getState() {
    return state;
  }

  public void setState(ServiceState state) {
    this.state = state;
  }
  
  public boolean equals(Object object) {
    if(object instanceof InterlokService) {
      if(((InterlokService) object).getId().equals(this.getId()))
        return true;
      else
        return false;
    } else
      return false;
  }

  public Exception getException() {
    return exception;
  }

  public void setException(Exception exception) {
    this.exception = exception;
  }
  
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("id", getId()).append("state", getState().name())
        .append("Exception", this.getException() != null).toString();
  }
}