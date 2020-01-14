package com.adaptris.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.SerializableAdaptrisMessage;

public class VertXMessageTranslatorTest {

  private VertXMessageTranslator messageTranslator;
  
  @Before
  public void setUp() throws Exception {
    messageTranslator = new VertXMessageTranslator();
  }
  
  @After
  public void tearDown() throws Exception {
    
  }
  
  @Test
  public void testTranslateFromAdaptrisMessage() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage("Test Payload");
    adaptrisMessage.addMessageHeader("header1", "value1");
    adaptrisMessage.addMessageHeader("header2", "value2");
    adaptrisMessage.addMessageHeader("header3", "value3");
    
    VertXMessage translatedVertxMessage = this.messageTranslator.translate(adaptrisMessage);
    
    assertTrue(translatedVertxMessage.getAdaptrisMessage() instanceof SerializableAdaptrisMessage);
    
    assertEquals("value1", translatedVertxMessage.getAdaptrisMessage().getMetadataValue("header1"));
    assertEquals("value2", translatedVertxMessage.getAdaptrisMessage().getMetadataValue("header2"));
    assertEquals("value3", translatedVertxMessage.getAdaptrisMessage().getMetadataValue("header3"));
  }
  
  @Test
  public void testTranslateFromVertxMessage() throws Exception {
    VertXMessage vertXMessage = new VertXMessage();
    SerializableAdaptrisMessage serializedMessage = new SerializableAdaptrisMessage();
    
    serializedMessage.setContent("Test Payload");
    serializedMessage.addMessageHeader("header1", "value1");
    serializedMessage.addMessageHeader("header2", "value2");
    serializedMessage.addMessageHeader("header3", "value3");
    
    vertXMessage.setAdaptrisMessage(serializedMessage);
    
    AdaptrisMessage adaptrisMessage = this.messageTranslator.translate(vertXMessage);
    
    assertTrue(adaptrisMessage instanceof AdaptrisMessage);
    
    assertEquals("value1", adaptrisMessage.getMetadataValue("header1"));
    assertEquals("value2", adaptrisMessage.getMetadataValue("header2"));
    assertEquals("value3", adaptrisMessage.getMetadataValue("header3"));
  }
  
  @Test
  public void testTranslatorNoSerializableTranslatorSet() throws Exception {
    VertXMessage vertXMessage = new VertXMessage();
    SerializableAdaptrisMessage serializedMessage = new SerializableAdaptrisMessage();
    
    serializedMessage.setContent("Test Payload");
    serializedMessage.addMessageHeader("header1", "value1");
    serializedMessage.addMessageHeader("header2", "value2");
    serializedMessage.addMessageHeader("header3", "value3");
    
    vertXMessage.setAdaptrisMessage(serializedMessage);
    
    try {
      this.messageTranslator.setSerializableMessageTranslator(null);
      this.messageTranslator.translate(vertXMessage);
      fail("No translator set, should fail.");
    } catch (CoreException ex) {
      // expected.
    }
  }
  
  @Test
  public void testTranslatorNoVertxMessage() throws Exception {
    VertXMessage vertXMessage = null;
    
    try {
      this.messageTranslator.translate(vertXMessage);
      fail("null vertx message, should fail.");
    } catch (CoreException ex) {
      // expected.
    }
  }
  
}
