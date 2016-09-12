package com.adaptris.vertx;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ComponentState;
import com.adaptris.core.CoreException;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("clustered-service-wrapper")
public class ClusteredServiceWrapper implements Service {

  private String uniqueId;
  
  private Service service;
  
  @Override
  public void init() throws CoreException {
    this.getService().init();
  }

  @Override
  public void start() throws CoreException {
    this.getService().start();
  }

  @Override
  public void stop() {
    this.getService().stop();
  }

  @Override
  public void close() {
    this.getService().close();
  }

  @Override
  public void prepare() throws CoreException {
    this.getService().prepare();
  }

  @Override
  public String createName() {
    return this.getService().createName();
  }

  @Override
  public String createQualifier() {
    return this.getService().createQualifier();
  }

  @Override
  public boolean isTrackingEndpoint() {
    return this.getService().isTrackingEndpoint();
  }

  @Override
  public boolean isConfirmation() {
    return this.getService().isConfirmation();
  }

  @Override
  public ComponentState retrieveComponentState() {
    return this.getService().retrieveComponentState();
  }

  @Override
  public void changeState(ComponentState newState) {
    this.getService().changeState(newState);
  }

  @Override
  public void requestInit() throws CoreException {
    this.getService().requestInit();
  }

  @Override
  public void requestStart() throws CoreException {
    this.getService().requestStart();
  }

  @Override
  public void requestStop() {
    this.getService().requestStop();
  }

  @Override
  public void requestClose() {
    this.getService().requestClose();
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setUniqueId(String uniqueId) {
    this.uniqueId = uniqueId;
  }

  @Override
  public String getUniqueId() {
    return uniqueId;
  }

  @Override
  public boolean isBranching() {
    return false;
  }

  @Override
  public boolean continueOnFailure() {
    return this.getService().continueOnFailure();
  }

  public Service getService() {
    return service;
  }

  public void setService(Service service) {
    this.service = service;
  }
  
}
