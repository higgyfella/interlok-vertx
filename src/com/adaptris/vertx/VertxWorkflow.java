package com.adaptris.vertx;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultSerializableMessageTranslator;
import com.adaptris.core.ProduceException;
import com.adaptris.core.SerializableAdaptrisMessage;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandardWorkflow;
import com.adaptris.core.licensing.License;
import com.adaptris.core.licensing.License.LicenseType;
import com.adaptris.core.licensing.LicenseChecker;
import com.adaptris.core.licensing.LicensedComponent;
import com.adaptris.core.util.ManagedThreadFactory;
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
  
  private static final int DEFAULT_QUEUE_SIZE = 100;
  
  private int queueCapacity;
  
  private VertXMessageTranslator vertXMessageTranslator;
  
  private MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private transient ArrayBlockingQueue<VertXMessage> processingQueue;
  
  private transient ExecutorService messageExecutor;
  
  private transient Future<?> messageExecutorHandle;
  
  private transient Vertx vertX;
  
  private transient EventBus eventBus;
  
  private transient VertxWorkflow handler;
  
  private transient DefaultSerializableMessageTranslator serializableMessageTranslator;

  public VertxWorkflow() {
    super();
    this.setQueueCapacity(DEFAULT_QUEUE_SIZE);
    this.setMessageCodec(new AdaptrisMessageCodec());
    processingQueue = new ArrayBlockingQueue<>(this.getQueueCapacity(), false);
    messageExecutor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
    handler = this;
    serializableMessageTranslator = new DefaultSerializableMessageTranslator();
  }
  
  @Override
  public void onAdaptrisMessage(AdaptrisMessage msg) {
    try {
      workflowStart(msg);
      log.debug("start processing msg [" + msg.toString(false) + "]");
      
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord(this.getServiceCollection()));
      translatedMessage.setStartProcessingTime(System.currentTimeMillis());
      
      processingQueue.put(translatedMessage);
    } catch (CoreException e) {
      log.error("Error processing message: ", e);
      handleBadMessage(msg);
    } catch (InterruptedException e) {
      log.error("Error processing message: ", e);
      handleBadMessage(msg);
    }
  }
  
  public void onVertxMessage(Message<VertXMessage> xMessage) {
    log.trace("Incoming message: " + xMessage);
    AdaptrisMessage adaptrisMessage = null;
    InterlokService nextService = null;
    VertXMessage vxMessage = null;
    try {
      vxMessage = xMessage.body();
      adaptrisMessage = this.getVertXMessageTranslator().translate(vxMessage);
      nextService = vxMessage.getServiceRecord().getNextService();
    } catch (CoreException | ServiceRecordException e) {
      log.error("Error translating incoming message.", e);
    }
    if(vxMessage.getServiceRecord().getLastRunService() != null) {
      if(vxMessage.getServiceRecord().getLastRunService().getState().equals(ServiceState.ERROR)) {
        this.handleBadMessage(adaptrisMessage);
        return ;
      }
    }
    if(nextService != null) { // finished?
      Service service = this.getServiceById(nextService.getId());
      try {
        service.doService(adaptrisMessage);
        nextService.setState(ServiceState.COMPLETE);
        vxMessage.getServiceRecord().setLastServiceId(service.getUniqueId());
        SerializableAdaptrisMessage serializableMessage = (SerializableAdaptrisMessage) serializableMessageTranslator.translate(adaptrisMessage);
        vxMessage.setAdaptrisMessage(serializableMessage);
      } catch (Exception exception) {
        log.error("Error running service.", exception);
        nextService.setState(ServiceState.ERROR);
        nextService.setException(exception);
      } finally {
        try {
          log.trace("Processed message, putting back on the queue: " + vxMessage);
          this.processingQueue.put(vxMessage);
        } catch (InterruptedException e) {
          log.error("Could not place message on the processing queue.  Probably shutting down.");
        }
      }
    } else {
      // finished processing.
      try {
        doProduce(adaptrisMessage);
        logSuccess(adaptrisMessage, xMessage.body().getStartProcessingTime());
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
  protected void initialiseWorkflow() throws CoreException {
    super.initialiseWorkflow();
    
    if(this.getVertXMessageTranslator() == null)
      this.setVertXMessageTranslator(new VertXMessageTranslator());
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
      // send it to vertx
      eventBus.send(this.getUniqueId(), xMessage);
    }
  }
  
  @Override
  public void handle(Message<VertXMessage> event) {
    if(event.body() != null)
      this.onVertxMessage(event);
  }
  
  private Service getServiceById(String id) {
    Service result = null;
    for(Service service : this.getServiceCollection().getServices()) {
      if(service.getUniqueId().equals(id)) {
        result = service;
        break;
      }
    }
    return result;
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

}
