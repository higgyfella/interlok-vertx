package com.adaptris.vertx;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.adaptris.core.Service;
import com.adaptris.core.ServiceCollection;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("service-record")
public class ServiceRecord {
  
  private List<InterlokService> services;

  private String lastServiceId;
  
  public ServiceRecord() {
    services = new ArrayList<InterlokService>();
  }
  
  public ServiceRecord(ServiceCollection serviceCollection) {
    this();
    for(Service service : serviceCollection) {
      services.add(new InterlokService(service.getUniqueId(), ServiceState.NOT_STARTED));
    }
  }
  
  public InterlokService getLastRunService() {
    if(!StringUtils.isEmpty(this.lastServiceId)) {
      int indexOfLastService = this.services.indexOf(new InterlokService(this.lastServiceId));
      if(indexOfLastService >= 0) {
        return this.services.get(indexOfLastService);
      }
    }
    return null;
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
  
  public InterlokService getNextService() throws ServiceRecordException {
    if(!StringUtils.isEmpty(this.lastServiceId)) {
      int indexOfLastService = this.services.indexOf(new InterlokService(this.lastServiceId));
      if(indexOfLastService >= 0) {
        if(services.size() > (indexOfLastService + 1)) {
          return services.get(indexOfLastService + 1); 
        } else
          return null; // No more services to run.
      } else
        throw new ServiceRecordException("Last run service not found: " + this.lastServiceId);
    } else {
      if(services.size() > 0) {
        return services.get(0);
      } else
        return null; // no services to run.
    }
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for(InterlokService service : services) {
      builder.append(service.toString());
      builder.append("\n");
    }
    return builder.toString();
  }

  public String getLastServiceId() {
    return lastServiceId;
  }

  public void setLastServiceId(String lastServiceId) {
    this.lastServiceId = lastServiceId;
  }
}
