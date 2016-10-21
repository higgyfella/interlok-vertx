package com.adaptris.vertx;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.concurrent.ArrayBlockingQueue;

import static org.mockito.Mockito.verify;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageProducerImp;
import com.adaptris.core.Channel;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.ProcessingExceptionHandler;
import com.adaptris.core.ProduceException;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.util.LifecycleHelper;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import junit.framework.TestCase;

public class VertxWorkflowTest extends TestCase {
  
  private VertxWorkflow vertxWorkflow;
  
  private ConstantDataInputParameter targetWorkflowId;
  
  private Channel channel;
  @Mock
  private ClusteredEventBus mockClusteredEventBus;
  @Mock
  private ProcessingExceptionHandler mockErrorHandler;
  @Mock
  private VertXMessageTranslator mockTranslator;
  @Mock
  private Message<VertXMessage> mockVertxMessage;
  @Mock
  private Message<Object> mockReplyVertxMessage;
  @Mock
  private Service mockService1, mockService2;
  @Mock
  private EventBus mockEventBus;
  @Mock
  private AdaptrisMessageProducerImp mockProducer;
  
  public void tearDown() throws Exception {
    LifecycleHelper.stop(channel);
    LifecycleHelper.close(channel);
  }
  
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    
    channel = new Channel();
    
    vertxWorkflow = new VertxWorkflow();
    targetWorkflowId = new ConstantDataInputParameter("SomeWorkflowID");
    vertxWorkflow.setTargetComponentId(targetWorkflowId);
    
    vertxWorkflow.setProducer(mockProducer);
    vertxWorkflow.setClusteredEventBus(mockClusteredEventBus);
    
    channel.getWorkflowList().add(vertxWorkflow);
    
    channel.prepare();
    LifecycleHelper.init(channel);
    LifecycleHelper.start(channel);
  }
  
  public void testOnMessageSendToCluster() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    
    vertxWorkflow.onAdaptrisMessage(adaptrisMessage);
  }
  
  public void testOnMessageCannotTranslateMessage() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    
    vertxWorkflow.registerActiveMsgErrorHandler(mockErrorHandler);
    
    vertxWorkflow.setVertXMessageTranslator(mockTranslator);
    when(mockTranslator.translate(adaptrisMessage))
        .thenThrow(new CoreException("GeneratedException"));
    
    vertxWorkflow.onAdaptrisMessage(adaptrisMessage);
    
    verify(mockErrorHandler).handleProcessingException(adaptrisMessage);
  }
  
  public void testReceivedVertxMessage() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxMessage.body())
        .thenReturn(vertXMessage);
    
    vertxWorkflow.onVertxMessage(mockVertxMessage);
    
    verify(mockVertxMessage).reply(vertXMessage);
  }
  
  public void testReceivedVertxMessageRunsServices() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxMessage.body())
        .thenReturn(vertXMessage);
    
    vertxWorkflow.getServiceCollection().add(mockService1);
    vertxWorkflow.getServiceCollection().add(mockService2);
    
    vertxWorkflow.onVertxMessage(mockVertxMessage);
    
    verify(mockService1).doService(any());
    verify(mockService2).doService(any());
    verify(mockVertxMessage).reply(vertXMessage);
  }
  
  public void testReceivedVertxMessageRunsServicesFirstServiceError() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxMessage.body())
        .thenReturn(vertXMessage);
    
    doThrow(new ServiceException("GeneratedException"))
        .when(mockService1).doService(any());
    
    vertxWorkflow.getServiceCollection().add(mockService1);
    vertxWorkflow.getServiceCollection().add(mockService2);
    
    vertxWorkflow.onVertxMessage(mockVertxMessage);
    
    verify(mockService1).doService(any());
    verify(mockService2, never()).doService(any());
    verify(mockVertxMessage).reply(vertXMessage);
  }
  
  public void testReceivedVertxMessageRunsServicesFirstServiceErrorContinueOnError() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    when(mockVertxMessage.body())
        .thenReturn(vertXMessage);
    
    doThrow(new ServiceException("GeneratedException"))
        .when(mockService1).doService(any());
    
    vertxWorkflow.getServiceCollection().add(mockService1);
    vertxWorkflow.getServiceCollection().add(mockService2);
    
    vertxWorkflow.setContinueOnError(true);
    
    vertxWorkflow.onVertxMessage(mockVertxMessage);
    
    verify(mockService1).doService(any());
    verify(mockService2).doService(any());
    verify(mockVertxMessage).reply(vertXMessage);
  }
  
  public void testOnMessageSendToSingle() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    ArrayBlockingQueue<VertXMessage> internalprocessingQueue = new ArrayBlockingQueue<>(1);
    internalprocessingQueue.put(vertXMessage);
    
    vertxWorkflow.setProcessingQueue(internalprocessingQueue);
    vertxWorkflow.getClusteredEventBus().setEventBus(mockEventBus);
    
    try {
      vertxWorkflow.processQueuedMessage();
    } catch (Exception ex) {
    } finally {
      verify(mockClusteredEventBus).send(any(), any());
    }
  }
  
  public void testOnMessageSendToAll() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    ArrayBlockingQueue<VertXMessage> internalprocessingQueue = new ArrayBlockingQueue<>(1);
    internalprocessingQueue.put(vertXMessage);
    
    vertxWorkflow.setTargetSendMode("ALL");
    vertxWorkflow.setProcessingQueue(internalprocessingQueue);
    vertxWorkflow.getClusteredEventBus().setEventBus(mockEventBus);
    
    try {
      vertxWorkflow.processQueuedMessage();
    } catch (Exception ex) {
    } finally {
      verify(mockClusteredEventBus).publish(any(), any());
    }
  }
  
  public void testHandleMessageReplyFailsTranslator() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    
    vertxWorkflow.registerActiveMsgErrorHandler(mockErrorHandler);
    vertxWorkflow.setVertXMessageTranslator(mockTranslator);
    
    when(mockReplyVertxMessage.body())
        .thenReturn(vertXMessage);
    when(mockTranslator.translate(vertXMessage))
        .thenThrow(new CoreException("GeneratedException"));
    
    vertxWorkflow.handleMessageReply(mockReplyVertxMessage);
    
    verify(mockProducer, never()).produce(adaptrisMessage);
  }
  
  public void testHandleMessageReplyHasErrors() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    vertXMessage.getServiceRecord().addService(new InterlokService("SomeId", ServiceState.ERROR));
    
    vertxWorkflow.registerActiveMsgErrorHandler(mockErrorHandler);
    
    when(mockReplyVertxMessage.body())
        .thenReturn(vertXMessage);
    
    vertxWorkflow.handleMessageReply(mockReplyVertxMessage);
    
    verify(mockErrorHandler).handleProcessingException(any(AdaptrisMessage.class));
  }
  
  public void testHandleMessageReplyDoesProduce() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    vertXMessage.getServiceRecord().addService(new InterlokService("SomeId", ServiceState.COMPLETE));
        
    when(mockReplyVertxMessage.body())
        .thenReturn(vertXMessage);
    when(mockProducer.createName())
        .thenReturn("name");
    
    vertxWorkflow.handleMessageReply(mockReplyVertxMessage);
    
    verify(mockProducer).produce(any(AdaptrisMessage.class));
  }
  
  public void testHandleMessageReplyProduceFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);
    vertXMessage.getServiceRecord().addService(new InterlokService("SomeId", ServiceState.COMPLETE));
        
    vertxWorkflow.registerActiveMsgErrorHandler(mockErrorHandler);
    
    when(mockReplyVertxMessage.body())
        .thenReturn(vertXMessage);
    when(mockProducer.createName())
        .thenReturn("name");
    doThrow(new ProduceException("Generated Exception"))
        .when(mockProducer).produce(any(AdaptrisMessage.class));
    
    vertxWorkflow.handleMessageReply(mockReplyVertxMessage);
    
    verify(mockProducer).produce(any(AdaptrisMessage.class));
    verify(mockErrorHandler).handleProcessingException(any(AdaptrisMessage.class));
  }

}
