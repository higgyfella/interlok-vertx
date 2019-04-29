package com.adaptris.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import com.adaptris.core.CoreException;

public class InterlokServiceTest {
  
  @Test
  public void testEquals() {
    InterlokService interlokService1 = new InterlokService("id1");
    InterlokService interlokService2 = new InterlokService("id1");
    
    assertEquals(interlokService1, interlokService2);
    assertEquals(interlokService1.hashCode(), interlokService2.hashCode());
    assertFalse(interlokService1.equals(new Object()));
    assertTrue(interlokService1.equals(interlokService1));
    assertFalse(interlokService1.equals(new InterlokService()));
  }
  
  @Test
  public void testEqualsTrueWithDifferentStates() {
    InterlokService interlokService1 = new InterlokService("id1");
    interlokService1.setState(ServiceState.COMPLETE);
    
    InterlokService interlokService2 = new InterlokService("id1");
    interlokService2.setState(ServiceState.NOT_STARTED);
    
    assertEquals(interlokService1, interlokService2);
    assertEquals(interlokService1.hashCode(), interlokService2.hashCode());
  }
  
  @Test
  public void testEqualsTrueWithExceptionSet() {
    InterlokService interlokService1 = new InterlokService("id1");
    interlokService1.setException(new CoreException("TestException"));;
    
    InterlokService interlokService2 = new InterlokService("id1");
    
    assertEquals(interlokService1, interlokService2);
    assertEquals(interlokService1.hashCode(), interlokService2.hashCode());
  }

  @Test
  public void testToString() {
    InterlokService srv1 = new InterlokService();
    assertNotNull(srv1.toString());
    srv1.setException(new Exception());
    assertNotNull(srv1.toString());
  }

}
