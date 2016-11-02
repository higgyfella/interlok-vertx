package com.adaptris.vertx;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.InterlokException;

import io.vertx.core.eventbus.Message;
import junit.framework.TestCase;

public class VertxServiceTest extends TestCase {
  
  private VertxService vertxService;
  
  private ConstantDataInputParameter targetComponentId;
  
  @Mock
  private ClusteredEventBus mockClusteredEventBus;
  @Mock
  private Service wrappedService;
  @Mock
  private Service replyService;
  @Mock
  private VertXMessageTranslator mockTranslator;
  @Mock
  private Message<VertXMessage> mockVertxMessage;
  @Mock
  private Message<Object> mockVertxReplyMessage;
  
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    
    vertxService = new VertxService();
    vertxService.setService(wrappedService);
    vertxService.setReplyService(replyService);
    vertxService.setClusteredEventBus(mockClusteredEventBus);
    
    targetComponentId = new ConstantDataInputParameter("SomeWorkflowID");
    vertxService.setTargetComponentId(targetComponentId);
    
    LifecycleHelper.init(vertxService);
    LifecycleHelper.start(vertxService);
  }
  
  public void tearDown() throws Exception {
    LifecycleHelper.stop(vertxService);
    LifecycleHelper.close(vertxService);
  }

  public void testDoServiceSend() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    
    vertxService.doService(adaptrisMessage);
    
    verify(mockClusteredEventBus).send(any(), any());
  }
  
  public void testDoServicePublish() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    
    vertxService.setTargetSendMode("ALL");
    vertxService.doService(adaptrisMessage);
    
    verify(mockClusteredEventBus).publish(any(), any());
  }
  
  public void testDoServiceTranslateFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    
    when(mockTranslator.translate(adaptrisMessage))
        .thenThrow(new CoreException("GeneratedException"));
    
    vertxService.setVertXMessageTranslator(mockTranslator);
    
    try {
      vertxService.doService(adaptrisMessage);
      fail("Exception expected");
    } catch (InterlokException ex) {
      // expected
    }
  }
  
  public void testDoServiceNoSendEndpoint() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    
    vertxService.setTargetComponentId(new ConstantDataInputParameter(""));

    vertxService.doService(adaptrisMessage);
    
    verify(wrappedService).doService(any(AdaptrisMessage.class));

  }
  
  public void testReceiveMessageServiceFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxMessage.body())
        .thenReturn(vertXMessage);
    
    doThrow(new ServiceException("Generated Exception"))
        .when(wrappedService).doService(any(AdaptrisMessage.class));

    vertxService.handle(mockVertxMessage);
    
    assertEquals(ServiceState.ERROR, vertXMessage.getServiceRecord().getServices().get(0).getState());
  }
  
  public void testReceiveMessageTranslateFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxMessage.body())
        .thenReturn(vertXMessage);
    
    when(mockTranslator.translate(vertXMessage))
        .thenThrow(new CoreException("GeneratedException"));

    vertxService.setVertXMessageTranslator(mockTranslator);

    vertxService.handle(mockVertxMessage);
    
    verify(wrappedService, never()).doService(any(AdaptrisMessage.class));
  }
  
  public void testReceiveReplyMessageTranslateFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxReplyMessage.body())
        .thenReturn(vertXMessage);
    
    when(mockTranslator.translate(vertXMessage))
        .thenThrow(new CoreException("GeneratedException"));

    vertxService.setVertXMessageTranslator(mockTranslator);

    vertxService.handleMessageReply(mockVertxReplyMessage);
    
    verify(replyService, never()).doService(any(AdaptrisMessage.class));
  }
  
  public void testReceiveReplyMessageRunsReplyService() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxReplyMessage.body())
        .thenReturn(vertXMessage);

    vertxService.handleMessageReply(mockVertxReplyMessage);
    
    verify(replyService).doService(any(AdaptrisMessage.class));
  }
  
  
}
