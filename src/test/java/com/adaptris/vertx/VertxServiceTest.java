package com.adaptris.vertx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.ProcessingExceptionHandler;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandardProcessingExceptionHandler;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.services.LogMessageService;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;

import io.vertx.core.eventbus.Message;

public class VertxServiceTest extends ExampleServiceCase {

  public static final String BASE_DIR_KEY = "ServiceCase.baseDir";

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
  @Mock
  private ProcessingExceptionHandler mockProcessingExceptionHandler;
  @Mock
  private DataInputParameter<String> mockDataInputParameter;

  private AutoCloseable closeable;

  @Before
  public void setUp() throws Exception {
    closeable = MockitoAnnotations.openMocks(this);

    vertxService = new VertxService();
    vertxService.setService(wrappedService);
    vertxService.setReplyService(replyService);
    vertxService.setClusteredEventBus(mockClusteredEventBus);
    vertxService.setMaxThreads(5);
    vertxService.setClusterId("myCluster");
    vertxService.setVertxProperties(null);
    // INTERLOK-1563 need to invoke the consumerStarted() method, to handle the countdownLatch
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        ((ConsumerEventListener) invocation.getArguments()[0]).consumerStarted();
        return null;
      }
    }).when(mockClusteredEventBus).startClusteredConsumer(vertxService, null);

    targetComponentId = new ConstantDataInputParameter("SomeWorkflowID");
    vertxService.setTargetComponentId(targetComponentId);

    LifecycleHelper.initAndStart(vertxService);
  }

  @After
  public void tearDown() throws Exception {
    LifecycleHelper.stopAndClose(vertxService);
    closeable.close();
  }

  @Test
  public void testDoServiceSend() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();

    vertxService.doService(adaptrisMessage);

    verify(mockClusteredEventBus).send(any(), any(), anyBoolean());
  }

  @Test
  public void testDoServicePublish() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();

    vertxService.setTargetSendMode(SendMode.Mode.ALL);
    vertxService.doService(adaptrisMessage);

    verify(mockClusteredEventBus).publish(any(), any());
  }

  @Test
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

  @Test
  public void testDoServiceTargetFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();

    doThrow(new InterlokException("expected"))
    .when(mockDataInputParameter).extract(adaptrisMessage);

    vertxService.setTargetComponentId(mockDataInputParameter);

    try {
      vertxService.doService(adaptrisMessage);
      fail("Exception expected");
    } catch (InterlokException ex) {
      // expected
    }
  }

  @Test
  public void testDoServiceNoSendEndpoint() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();

    vertxService.setTargetComponentId(new ConstantDataInputParameter(""));

    vertxService.doService(adaptrisMessage);

    verify(wrappedService).doService(any(AdaptrisMessage.class));

  }

  @Test
  public void testReceiveMessageServiceFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);

    when(mockVertxMessage.body())
    .thenReturn(vertXMessage);

    doThrow(new ServiceException("Generated Exception"))
    .when(wrappedService).doService(any(AdaptrisMessage.class));

    vertxService.handle(mockVertxMessage);

    Awaitility.await().until(() -> vertXMessage.getServiceRecord().getServices().size() > 0);

    assertEquals(ServiceState.ERROR, vertXMessage.getServiceRecord().getServices().get(0).getState());
  }

  @Test
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

  @Test
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

  @Test
  public void testReceiveReplyMessageRunsReplyService() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);

    when(mockVertxReplyMessage.body())
    .thenReturn(vertXMessage);

    vertxService.handleMessageReply(mockVertxReplyMessage);

    verify(replyService).doService(any(AdaptrisMessage.class));
  }

  @Test
  public void testReceiveReplyMessageRunsReplyServiceFails() throws Exception {
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage();
    VertXMessage vertXMessage = new VertXMessageTranslator().translate(adaptrisMessage);

    vertxService.setReplyServiceExceptionHandler(mockProcessingExceptionHandler);

    when(mockVertxReplyMessage.body())
    .thenReturn(vertXMessage);
    doThrow(new ServiceException("GeneratedException"))
    .when(replyService).doService(any(AdaptrisMessage.class));

    vertxService.handleMessageReply(mockVertxReplyMessage);

    verify(replyService).doService(any(AdaptrisMessage.class));
    verify(mockProcessingExceptionHandler).handleProcessingException(any(AdaptrisMessage.class));
  }

  @Override
  protected Object retrieveObjectForSampleConfig() {
    VertxService vertxService = new VertxService();
    vertxService.setUniqueId("MyServiceCluster");
    vertxService.setService(new LogMessageService());
    vertxService.setReplyService(new LogMessageService());
    vertxService.setReplyServiceExceptionHandler(new StandardProcessingExceptionHandler(new LogMessageService()));
    vertxService.setTargetComponentId(new ConstantDataInputParameter("my-cluster-name"));

    return vertxService;
  }
}
