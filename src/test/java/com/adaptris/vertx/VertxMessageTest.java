package com.adaptris.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import com.adaptris.core.SerializableAdaptrisMessage;

public class VertxMessageTest {
  
  @Test
  public void testEquals() {
    SerializableAdaptrisMessage msg = new SerializableAdaptrisMessage("id1");
    VertXMessage m1 = new VertXMessage(msg);
    VertXMessage m2 = new VertXMessage(msg);
    
    assertEquals(m1, m2);
    assertEquals(m1.hashCode(), m2.hashCode());
    assertTrue(m1.equals(m1));
    assertFalse(m1.equals(new Object()));
    assertNotEquals(m1, new VertXMessage());
  }
  

  @Test
  public void testToString() {
    SerializableAdaptrisMessage msg = new SerializableAdaptrisMessage("id1");
    VertXMessage m1 = new VertXMessage(msg);
    assertNotNull(m1.toString());
  }

}
