package com.adaptris.vertx;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.adaptris.core.SerializableAdaptrisMessage;
import com.adaptris.util.GuidGenerator;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("clustered-message")
public class VertXMessage {
  
  private SerializableAdaptrisMessage adaptrisMessage;
    
  private ServiceRecord serviceRecord;
  
  private long startProcessingTime;
  
  public VertXMessage() {
    serviceRecord = new ServiceRecord();
    adaptrisMessage = new SerializableAdaptrisMessage(new GuidGenerator().getUUID());
  }
  
  public SerializableAdaptrisMessage getAdaptrisMessage() {
    return adaptrisMessage;
  }

  public void setAdaptrisMessage(SerializableAdaptrisMessage adaptrisMessage) {
    this.adaptrisMessage = adaptrisMessage;
  }

  public ServiceRecord getServiceRecord() {
    return serviceRecord;
  }

  public void setServiceRecord(ServiceRecord serviceRecord) {
    this.serviceRecord = serviceRecord;
  }

  public long getStartProcessingTime() {
    return startProcessingTime;
  }

  public void setStartProcessingTime(long startProcessingTime) {
    this.startProcessingTime = startProcessingTime;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Start processing at :" + this.getStartProcessingTime());
    builder.append("\n");
    builder.append("Service Record :\n" + this.getServiceRecord());
    builder.append("\n");
    builder.append("\n");
    return builder.toString();
  }
  
  /*
   * If the unique id's are the same then equals will return true.
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object object) {
    if (object == this)
      return true;
    if (object instanceof VertXMessage) {
      VertXMessage otherInstance = (VertXMessage) object;
      return new EqualsBuilder()
            .append(this.getAdaptrisMessage().getUniqueId(), otherInstance.getAdaptrisMessage().getUniqueId())
            .isEquals();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.getAdaptrisMessage().getUniqueId().hashCode();
  }
}
