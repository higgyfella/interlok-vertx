package com.adaptris.vertx;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;

public interface MessageTranslator {

  public VertXMessage translate(AdaptrisMessage adaptrisMessage) throws CoreException;
  
  public AdaptrisMessage translate(VertXMessage vertxMessage) throws CoreException;
  
}
