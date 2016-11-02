package com.adaptris.vertx;

import com.adaptris.core.CoreException;

import junit.framework.TestCase;

public class InterlokServiceTest extends TestCase {
  
  public void testEquals() {
    InterlokService interlokService1 = new InterlokService("id1");
    InterlokService interlokService2 = new InterlokService("id1");
    
    assertEquals(interlokService1, interlokService2);
  }
  
  public void testEqualsTrueWithDifferentStates() {
    InterlokService interlokService1 = new InterlokService("id1");
    interlokService1.setState(ServiceState.COMPLETE);
    
    InterlokService interlokService2 = new InterlokService("id1");
    interlokService2.setState(ServiceState.NOT_STARTED);
    
    assertEquals(interlokService1, interlokService2);
  }
  
  public void testEqualsTrueWithExceptionSet() {
    InterlokService interlokService1 = new InterlokService("id1");
    interlokService1.setException(new CoreException("TestException"));;
    
    InterlokService interlokService2 = new InterlokService("id1");
    
    assertEquals(interlokService1, interlokService2);
  }

}
