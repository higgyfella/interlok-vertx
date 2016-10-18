package com.adaptris.vertx;

import java.util.ArrayList;
import java.util.List;

import com.adaptris.core.Service;
import com.adaptris.core.ServiceCollection;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("service-record")
public class ServiceRecord {
  
  private List<InterlokService> services;
  
  public ServiceRecord() {
    services = new ArrayList<InterlokService>();
  }
  
  public ServiceRecord(ServiceCollection serviceCollection) {
    this();
    for(Service service : serviceCollection) {
      services.add(new InterlokService(service.getUniqueId(), ServiceState.NOT_STARTED));
    }
  }
  
  public ServiceRecord(List<String> serviceIds) {
    this();
    for(String serviceId : serviceIds) {
      services.add(new InterlokService(serviceId, ServiceState.NOT_STARTED));
    }
  }
  
  public boolean isSuccessfullyComplete() {
    boolean result = true;
    for(InterlokService service : services) {
      if(!service.getState().equals(ServiceState.COMPLETE)) {
        result = false;
        break;
      }
    }
    return result;
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
