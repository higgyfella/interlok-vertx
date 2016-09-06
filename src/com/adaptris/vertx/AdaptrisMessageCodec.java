package com.adaptris.vertx;

import org.apache.activemq.util.ByteArrayInputStream;

import com.adaptris.core.AdaptrisMarshaller;
import com.adaptris.core.CoreException;
import com.adaptris.core.XStreamJsonMarshaller;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

@XStreamAlias("adaptris-message-codec")
public class AdaptrisMessageCodec implements MessageCodec<VertXMessage, VertXMessage> {
  
  private static final String CODEC_NAME = "AdaptrisVertXMessageCodec";
  
  private AdaptrisMarshaller marshaller;

  public AdaptrisMessageCodec() {
    try {
      marshaller = new XStreamJsonMarshaller();
    } catch (CoreException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void encodeToWire(Buffer buffer, VertXMessage xMessage) {
    try {
      String serialisedString = marshaller.marshal(xMessage);
      buffer.appendBytes(serialisedString.getBytes());
    } catch (CoreException e) {
      e.printStackTrace();
    }
  }

  @Override
  public VertXMessage decodeFromWire(int pos, Buffer buffer) {
    try {
      return (VertXMessage) marshaller.unmarshal(new ByteArrayInputStream(buffer.getBytes()));
    } catch (CoreException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public VertXMessage transform(VertXMessage xMessage) {
    
    return null;
  }

  @Override
  public String name() {
    return CODEC_NAME;
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }

  public AdaptrisMarshaller getMarshaller() {
    return marshaller;
  }

  public void setMarshaller(AdaptrisMarshaller marshaller) {
    this.marshaller = marshaller;
  }
  
}
