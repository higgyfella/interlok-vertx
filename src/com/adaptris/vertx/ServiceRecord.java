package com.adaptris.vertx;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("clustered-service-record")
public class ServiceRecord {
  
  private List<InterlokService> services;
  
  public ServiceRecord() {
    services = new ArrayList<InterlokService>();
  }
  
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("services", services).toString();
  }

  public void addService(InterlokService interlokService) {
    this.services.add(interlokService);
  }

  public List<InterlokService> getServices() {
    return services;
  }

  public void setServices(List<InterlokService> services) {
    this.services = services;
  }
}
