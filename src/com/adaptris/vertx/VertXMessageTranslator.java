package com.adaptris.vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultSerializableMessageTranslator;
import com.adaptris.core.SerializableAdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("clustered-message-translator")
public class VertXMessageTranslator implements MessageTranslator {
  
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());
    
  private transient DefaultSerializableMessageTranslator serializableMessageTranslator;
  
  public VertXMessageTranslator() throws CoreException {
    this.setSerializableMessageTranslator(new DefaultSerializableMessageTranslator());
  }

  @Override
  public VertXMessage translate(AdaptrisMessage adaptrisMessage) throws CoreException {
    VertXMessage vertXMessage = new VertXMessage();
    
    SerializableAdaptrisMessage serializableMessage = (SerializableAdaptrisMessage) this.getSerializableMessageTranslator().translate(adaptrisMessage);
    vertXMessage.setAdaptrisMessage(serializableMessage);
    return vertXMessage;
  }

  @Override
  public AdaptrisMessage translate(VertXMessage vertxMessage) throws CoreException {
    if(this.getSerializableMessageTranslator() == null)
      throw new CoreException("Serializer is null");
    if(vertxMessage == null)
      throw new CoreException("VertXMessage is null");
    return this.getSerializableMessageTranslator().translate(vertxMessage.getAdaptrisMessage());
  }

  public DefaultSerializableMessageTranslator getSerializableMessageTranslator() {
    return serializableMessageTranslator;
  }

  public void setSerializableMessageTranslator(DefaultSerializableMessageTranslator serializableMessageTranslator) {
    this.serializableMessageTranslator = serializableMessageTranslator;
  }

}
