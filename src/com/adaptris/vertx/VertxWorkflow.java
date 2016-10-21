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

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;

@XStreamAlias("vertx-workflow")
public class VertxWorkflow extends StandardWorkflow implements Handler<Message<VertXMessage>>, ConsumerEventListener, LicensedComponent {
  
  private enum SEND_MODE {
    ALL,
    SINGLE;
  }
  
  private static final String DEFAULT_SEND_MODE = SEND_MODE.SINGLE.name();
  
  private static final int DEFAULT_QUEUE_SIZE = 10;
  
  private int queueCapacity;
  
  private VertXMessageTranslator vertXMessageTranslator;
  
  private MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private DataInputParameter<String> targetComponentId;
  
  private Boolean continueOnError;
  
  private String targetSendMode;
  
  private transient ArrayBlockingQueue<VertXMessage> processingQueue;
  
  private transient ExecutorService messageExecutor;
  
  private transient Future<?> messageExecutorHandle;
      
  private transient ClusteredEventBus clusteredEventBus;
  
  public VertxWorkflow() {
    super();
    this.setQueueCapacity(DEFAULT_QUEUE_SIZE);
    this.setMessageCodec(new AdaptrisMessageCodec());
    messageExecutor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
    this.setTargetSendMode(DEFAULT_SEND_MODE);
    clusteredEventBus = new ClusteredEventBus();
    clusteredEventBus.setMessageCodec(getMessageCodec());
  }
  
  @Override
  public void onAdaptrisMessage(AdaptrisMessage msg) {
    try {
      workflowStart(msg);
      log.debug("start processing msg [" + msg.toString(false) + "]");
      
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord());
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
    
    try {
      VertXMessage vertXMessage = this.getVertXMessageTranslator().translate(adaptrisMessage);
      vxMessage.setAdaptrisMessage(vertXMessage.getAdaptrisMessage());
      xMessage.reply(vxMessage);
    } catch (CoreException e) {
      log.error("Could not translate the Vertx Message to an AdaptrisMessage", e);
    }    
  }

  @Override
  protected void initialiseWorkflow() throws CoreException {
    super.initialiseWorkflow();
    
    this.getClusteredEventBus().setConsumerEventListener(this);
    
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
    
    clusteredEventBus.startClusteredConsumer(this.getUniqueId());
  }
  
  @Override
  public void consumerStarted() {
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

  void processQueuedMessage() throws InterruptedException {
    VertXMessage xMessage = processingQueue.poll(1L, TimeUnit.SECONDS);
    
    if(xMessage != null) {
      this.reportQueue("after a get [" + xMessage.getAdaptrisMessage().getUniqueId() + "]");
      // send it to vertx   
      try {
        if(this.getTargetSendMode().equalsIgnoreCase(SEND_MODE.SINGLE.name())) {
          getClusteredEventBus().send(targetComponentId(xMessage), xMessage);
        } else {
          getClusteredEventBus().publish(targetComponentId(xMessage), xMessage);
        }
      } catch (InterlokException exception) {
        log.error("Cannot derive the target from the incoming message.", exception);
      }
    }
  }
  
  public void handleMessageReply(Message<Object> result) {
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
    if(getClusteredEventBus().getEventBus() != null)
      getClusteredEventBus().getEventBus().consumer(this.getUniqueId()).unregister();
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
  
  protected String targetComponentId(VertXMessage vertxMessage) throws InterlokException {
    AdaptrisMessage adaptrisMessage = this.getVertXMessageTranslator().translate(vertxMessage);
    return this.getTargetComponentId().extract(adaptrisMessage);
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

  public DataInputParameter<String> getTargetComponentId() {
    return targetComponentId;
  }

  public void setTargetComponentId(DataInputParameter<String> targetComponentId) {
    this.targetComponentId = targetComponentId;
  }

  public ClusteredEventBus getClusteredEventBus() {
    return clusteredEventBus;
  }

  public void setClusteredEventBus(ClusteredEventBus clusteredEventBus) {
    this.clusteredEventBus = clusteredEventBus;
  }

  public ArrayBlockingQueue<VertXMessage> getProcessingQueue() {
    return processingQueue;
  }

  public void setProcessingQueue(ArrayBlockingQueue<VertXMessage> processingQueue) {
    this.processingQueue = processingQueue;
  }

}
