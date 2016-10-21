package com.adaptris.vertx;

import java.util.ArrayList;
import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("service-record")
public class ServiceRecord {
  
  private List<InterlokService> services;
  
  public ServiceRecord() {
    services = new ArrayList<InterlokService>();
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for(InterlokService service : services) {
      builder.append(service.toString());
      builder.append("\n");
    }
    return builder.toString();
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
