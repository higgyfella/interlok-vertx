package com.adaptris.vertx;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandardWorkflow;
import com.adaptris.core.licensing.License;
import com.adaptris.core.licensing.License.LicenseType;
import com.adaptris.core.licensing.LicenseChecker;
import com.adaptris.core.licensing.LicensedComponent;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;

@XStreamAlias("vertx-workflow")
public class VertxWorkflow extends StandardWorkflow implements Handler<Message<VertXMessage>>, LicensedComponent {
  
  private enum SEND_MODE {
    ALL,
    SINGLE;
  }
  
  private static final String DEFAULT_SEND_MODE = SEND_MODE.SINGLE.name();
  
  private static final int DEFAULT_QUEUE_SIZE = 10;
  
  private int queueCapacity;
  
  private VertXMessageTranslator vertXMessageTranslator;
  
  private MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private DataInputParameter<String> targetWorkflowId;
  
  private Boolean continueOnError;
  
  private String targetSendMode;
  
  private transient ArrayBlockingQueue<VertXMessage> processingQueue;
  
  private transient ExecutorService messageExecutor;
  
  private transient Future<?> messageExecutorHandle;
  
  private transient Vertx vertX;
  
  private transient EventBus eventBus;
  
  private transient VertxWorkflow handler;
  
  public VertxWorkflow() {
    super();
    this.setQueueCapacity(DEFAULT_QUEUE_SIZE);
    this.setMessageCodec(new AdaptrisMessageCodec());
    messageExecutor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
    handler = this;
    this.setTargetSendMode(DEFAULT_SEND_MODE);
  }
  
  @Override
  public void onAdaptrisMessage(AdaptrisMessage msg) {
    try {
      workflowStart(msg);
      log.debug("start processing msg [" + msg.toString(false) + "]");
      
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord(this.getServiceCollection()));
      translatedMessage.setStartProcessingTime(System.currentTimeMillis());
      
      log.trace("New message [" + msg.getUniqueId() + "] ::: Queue slots available: " +  processingQueue.remainingCapacity());
      processingQueue.put(translatedMessage);
      log.trace("New queue size : " +  processingQueue.remainingCapacity());
      this.reportQueue("new message put [" + msg.getUniqueId() + "]");
    } catch (CoreException e) {
      log.error("Error processing message: ", e);
      handleBadMessage(msg);
    } catch (InterruptedException e) {
      log.error("Error processing message: ", e);
      handleBadMessage(msg);
    }
  }
  
  public void onVertxMessage(Message<VertXMessage> xMessage) {
    AdaptrisMessage adaptrisMessage = null;
    VertXMessage vxMessage = null;
    try {
      vxMessage = xMessage.body();
      adaptrisMessage = this.getVertXMessageTranslator().translate(vxMessage);
      log.trace("Incoming message: " + adaptrisMessage.getUniqueId());
    } catch (CoreException e) {
      log.error("Error translating incoming message.", e);
    }

    for(Service service : this.getServiceCollection()) {
      InterlokService interlokService = new InterlokService(service.getUniqueId());
      
      try {
        service.doService(adaptrisMessage);
        interlokService.setState(ServiceState.COMPLETE);
      } catch (ServiceException ex) {
        log.error("Error running service.", ex);
        interlokService.setState(ServiceState.ERROR);
        interlokService.setException(ex);
        if(!continueOnError())
          break;
      } finally {
        vxMessage.getServiceRecord().addService(interlokService);
      }
    }
    
    xMessage.reply(vxMessage);
  }

  @Override
  protected void initialiseWorkflow() throws CoreException {
    super.initialiseWorkflow();
    
    if(this.getVertXMessageTranslator() == null)
      this.setVertXMessageTranslator(new VertXMessageTranslator());
    
    processingQueue = new ArrayBlockingQueue<>(this.getQueueCapacity(), true);
  }

  @Override
  protected void prepareWorkflow() throws CoreException {
    LicenseChecker.newChecker().checkLicense(this);
    super.prepareWorkflow();
  }

  @Override
  protected void resubmitMessage(AdaptrisMessage msg) {
    super.resubmitMessage(msg);
  }

  @Override
  protected void startWorkflow() throws CoreException {
    super.startWorkflow();
    
    Vertx.clusteredVertx(new VertxOptions(), new Handler<AsyncResult<Vertx>>() {
      @Override
      public void handle(AsyncResult<Vertx> event) {
        vertX = event.result();
        eventBus = vertX.eventBus();
        eventBus.registerDefaultCodec(VertXMessage.class, getMessageCodec());
        eventBus.consumer(getUniqueId(), handler);
        
        Runnable messageProcessorRunnable = new Runnable() {
          @Override
          public void run() {
            boolean interrupted = false;
            while((!messageExecutorHandle.isDone()) && (!interrupted)) {
              try {
                processQueuedMessage();
              } catch (InterruptedException e) {
                interrupted = true;
              }
            }
          }
        };
        
        messageExecutorHandle = messageExecutor.submit(messageProcessorRunnable);
      }
    });

  }

  private void processQueuedMessage() throws InterruptedException {
    VertXMessage xMessage = processingQueue.poll(1L, TimeUnit.SECONDS);
    
    if(xMessage != null) {
      this.reportQueue("after a get [" + xMessage.getAdaptrisMessage().getUniqueId() + "]");
      // send it to vertx   
      try {
        if(this.getTargetSendMode().equalsIgnoreCase(SEND_MODE.SINGLE.name())) {
          eventBus.send(targetWorkflowId(xMessage), xMessage, replyHandler -> {
            if (replyHandler.succeeded()) {
              handleReply(replyHandler.result());
            }
          });
        } else {
          eventBus.publish(targetWorkflowId(xMessage), xMessage);
        }
      } catch (InterlokException exception) {
        log.error("Cannot derive the target from the incoming message.", exception);
      }
    }
  }
  
  private void handleReply(Message<Object> result) {
    VertXMessage resultMessage = (VertXMessage) result.body();
    
    AdaptrisMessage adaptrisMessage;
    try {
      adaptrisMessage = this.getVertXMessageTranslator().translate(resultMessage);
    } catch (CoreException e) {
      log.error("Cannot translate the reply message back to an AdaptrisMessage", e);
      return;
    }
    log.debug("Received reply: " + resultMessage.getAdaptrisMessage().getUniqueId());
    log.trace(resultMessage.getAdaptrisMessage().getUniqueId() + ": Service record;\n" + resultMessage.getServiceRecord());
    
    boolean handleError = false;
    for(InterlokService service : resultMessage.getServiceRecord().getServices()) {
      if(service.getState().equals(ServiceState.ERROR)) {
        handleError = true;
        handleBadMessage("Exception from ServiceCollection", service.getException(), adaptrisMessage);
      }
    }
    
    if(!handleError) {
      try {
        doProduce(adaptrisMessage);
        logSuccess(adaptrisMessage, resultMessage.getStartProcessingTime());
      } catch (ServiceException e) {
        handleBadMessage("Exception from ServiceCollection", e, adaptrisMessage);
      } catch (ProduceException e) {
        adaptrisMessage.addEvent(getProducer(), false); // generate event
        handleBadMessage("Exception producing msg", e, adaptrisMessage);
        handleProduceException();
      } finally {
        sendMessageLifecycleEvent(adaptrisMessage);
      }
      workflowEnd(adaptrisMessage, adaptrisMessage);
    }
  }

  @Override
  public void handle(Message<VertXMessage> event) {
    if(event.body() != null)
      this.onVertxMessage(event);
  }
  
  @Override
  public boolean isEnabled(License license) {
    return license.isEnabled(LicenseType.Standard);
  }
  
  @Override
  protected void stopWorkflow() {
    super.stopWorkflow();
    if(messageExecutorHandle != null)
      messageExecutorHandle.cancel(false);
    if(eventBus != null)
      eventBus.consumer(this.getUniqueId()).unregister();
  }
  
  @Override
  protected void closeWorkflow() {
    super.closeWorkflow();
    if(messageExecutorHandle != null) {
      if(!messageExecutorHandle.isCancelled())
        messageExecutorHandle.cancel(true);
    }
  }
  

  public int getQueueCapacity() {
    return queueCapacity;
  }

  public void setQueueCapacity(int queueCapacity) {
    this.queueCapacity = queueCapacity;
  }

  public VertXMessageTranslator getVertXMessageTranslator() {
    return vertXMessageTranslator;
  }

  public void setVertXMessageTranslator(VertXMessageTranslator vertXMessageTranslator) {
    this.vertXMessageTranslator = vertXMessageTranslator;
  }

  public MessageCodec<VertXMessage, VertXMessage> getMessageCodec() {
    return messageCodec;
  }

  public void setMessageCodec(MessageCodec<VertXMessage, VertXMessage> messageCodec) {
    this.messageCodec = messageCodec;
  }
  
  protected String targetWorkflowId(VertXMessage vertxMessage) throws InterlokException {
    AdaptrisMessage adaptrisMessage = this.getVertXMessageTranslator().translate(vertxMessage);
    return this.getTargetWorkflowId().extract(adaptrisMessage);
  }

  public DataInputParameter<String> getTargetWorkflowId() {
    return targetWorkflowId;
  }

  public void setTargetWorkflowId(DataInputParameter<String> targetWorkflowId) {
    this.targetWorkflowId = targetWorkflowId;
  }

  private void reportQueue(String title) {
    StringBuilder builder = new StringBuilder();
    builder.append("\nCurrent Queue State (" + title + "):\n");
    
    VertXMessage[] array = new VertXMessage[processingQueue.size()];
    array = (VertXMessage[]) this.processingQueue.toArray(array);
    for(VertXMessage message : array) {
      builder.append("\t" + message.getAdaptrisMessage().getUniqueId() + "\n");
    }
    log.trace(builder.toString());
  }

  protected boolean continueOnError() {
    return this.getContinueOnError() != null ? this.getContinueOnError() : false;
  }
  
  public Boolean getContinueOnError() {
    return continueOnError;
  }

  public void setContinueOnError(Boolean continueOnError) {
    this.continueOnError = continueOnError;
  }

  public String getTargetSendMode() {
    return targetSendMode;
  }

  public void setTargetSendMode(String targetSendMode) {
    this.targetSendMode = targetSendMode;
  }
}
