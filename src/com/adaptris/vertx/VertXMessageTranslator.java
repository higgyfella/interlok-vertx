package com.adaptris.vertx;

import com.adaptris.core.AdaptrisMarshaller;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultSerializableMessageTranslator;
import com.adaptris.core.SerializableAdaptrisMessage;
import com.adaptris.core.XStreamJsonMarshaller;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("vertx-message-translator")
public class VertXMessageTranslator implements MessageTranslator {
  
  private AdaptrisMarshaller marshaller;
  
  private transient DefaultSerializableMessageTranslator defaultSerializableMessageTranslator;
  
  public VertXMessageTranslator() throws CoreException {
    marshaller = new XStreamJsonMarshaller();
  }

  @Override
  public VertXMessage translate(AdaptrisMessage adaptrisMessage) throws CoreException {
    VertXMessage vertXMessage = new VertXMessage();
    
    SerializableAdaptrisMessage serializableMessage = (SerializableAdaptrisMessage) defaultSerializableMessageTranslator.translate(adaptrisMessage);
    vertXMessage.setAdaptrisMessage(serializableMessage);
    return vertXMessage;
  }

  @Override
  public AdaptrisMessage translate(VertXMessage vertxMessage) throws CoreException {
    return defaultSerializableMessageTranslator.translate(vertxMessage.getAdaptrisMessage());
  }
  
  public AdaptrisMarshaller getMarshaller() {
    return marshaller;
  }

  public void setMarshaller(AdaptrisMarshaller marshaller) {
    this.marshaller = marshaller;
  }

}
