package com.adaptris.vertx;

import org.apache.activemq.util.ByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMarshaller;
import com.adaptris.core.CoreException;
import com.adaptris.core.XStreamJsonMarshaller;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

@XStreamAlias("clustered-adaptris-message-codec")
public class AdaptrisMessageCodec implements MessageCodec<VertXMessage, VertXMessage> {

  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  private static final String CODEC_NAME = "AdaptrisVertXMessageCodec";

  private AdaptrisMarshaller marshaller;

  public AdaptrisMessageCodec() {
    marshaller = new XStreamJsonMarshaller();
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
      return (VertXMessage) marshaller.unmarshal(new ByteArrayInputStream(buffer.getBytes(pos, buffer.length())));
    } catch (CoreException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public VertXMessage transform(VertXMessage xMessage) {
    return xMessage;
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
