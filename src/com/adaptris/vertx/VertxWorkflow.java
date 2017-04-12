package com.adaptris.vertx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
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
import com.adaptris.util.TimeInterval;
import com.adaptris.vertx.util.BlockingExpiryQueue;
import com.adaptris.vertx.util.ExpiryListener;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;

/**
 * <p>
 * A clustered workflow that allows you to farm out the service-list processing to a random instance of this workflow in your
 * cluster.<br/>
 * Clusters are managed and discovered by Hazelcast. To create a cluster you simply need to have multiple instances of this workflow
 * either in different channels or different instances of Interlok with the same unique-id on each instance of the workflow.
 * </p>
 * <p>
 * There are two modes of clustering; "all" and "single" (default), configured with the target-send-mode option.<br/>
 * If you select "all", then each message consumed by this workflow will be sent to ALL instances in the cluster. Likewise if you
 * select "single" then a single random workflow instance will process the message.<br/>
 * Additionally if you choose "single" once the worklfow instance has finished running their service-list the original worklfow will
 * receive the processed message as a reply. If you have a configured producer on the original workflow then the reply message will
 * be produced.<br/>
 * No reply is received if you choose "all" as the target send mode. Any producers you need must therefore be configured in the
 * service-list of each instance in the cluster.
 * </p>
 * <p>
 * You can choose the cluster to send any consumed messages to by configuring the target-component-id. The value of which will match
 * the cluster name (unique-id) of any clustered workflow.<br/>
 * Any consumed message will be sent to the named cluster, which may also include the workflow that consumed the message if that
 * workflow also shares the same unique-id as the other clustered workflow instances.
 * </p>
 * <p>
 * Every message consumed as mentioned earlier is sent to the cluster for processing and then for a potential reply, which means
 * that any consumed message is immediately acknowledged before the processing is complete. To stop the workflows consumer from
 * consuming too many messages waiting for processing we can set the maximum number of messages to be queued up for processing. The
 * default value is 10, but can be changed with the configuration option queue-capacity.<br/>
 * </p>
 * <p>
 * Finally, should a service in the clustered instance fail, further services will not be run, unless you configure
 * continue-on-error = true. The default value being false. When all services have executed successfully or one has failed therefore
 * stopping service execution, the reply is sent back to the original consumed workflow. Once the reply has been received it will
 * check to see if any services failed. If any services have failed then the message-error-handler will run.
 * </p>
 * 
 * @license ENTERPRISE
 * @config clustered-workflow
 * @since 3.5.0
 * @author Aaron
 *
 */
@AdapterComponent
@ComponentProfile(summary = "A workflow that allows clustered processing of services.", tag = "workflow,clustering,vertx")
@XStreamAlias("clustered-workflow")
public class VertxWorkflow extends StandardWorkflow implements Handler<Message<VertXMessage>>, ConsumerEventListener, LicensedComponent, ExpiryListener<VertXMessage> {
  
  private enum SEND_MODE {
    ALL,
    SINGLE;
  }
  
  private static final long DEFAULT_ITEM_EXPIRY_SECONDS = 30;
  
  private static final String DEFAULT_SEND_MODE = SEND_MODE.SINGLE.name();
  
  private static final int DEFAULT_QUEUE_SIZE = 10;
  
  @InputFieldDefault(value = "10")
  private int queueCapacity;
  
  @NotNull
  @Valid
  private DataInputParameter<String> targetComponentId;
  
  @InputFieldDefault(value = "false")
  private Boolean continueOnError;
  
  @NotNull
  @AutoPopulated
  private SendMode.Mode targetSendMode;
  
  @AutoPopulated
  @AdvancedConfig
  private VertXMessageTranslator vertXMessageTranslator;
  
  @NotNull
  @AutoPopulated
  private TimeInterval itemExpiryTimeout;
  
  private transient MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private transient ArrayBlockingQueue<VertXMessage> processingQueue;
  
  private transient BlockingExpiryQueue<VertXMessage> consumerQueue;
  
  private transient ExecutorService messageExecutor;
  
  private transient Future<?> messageExecutorHandle;
      
  private transient ClusteredEventBus clusteredEventBus;
  
  private transient Map<String, Map<Object, Object>> objectMetadataCache;
  
  public VertxWorkflow() {
    super();
    this.setQueueCapacity(DEFAULT_QUEUE_SIZE);
    this.setMessageCodec(new AdaptrisMessageCodec());
    messageExecutor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
    this.setTargetSendMode(SendMode.Mode.SINGLE);
    clusteredEventBus = new ClusteredEventBus();
    clusteredEventBus.setMessageCodec(getMessageCodec());
    objectMetadataCache = new HashMap<>();
  }
  
  @Override
  public void onAdaptrisMessage(AdaptrisMessage msg) {
    try {
      workflowStart(msg);
      log.debug("start processing msg [" + msg.toString(false) + "]");
      
      objectMetadataCache.put(msg.getUniqueId(), msg.getObjectHeaders()); 
      
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord());
      translatedMessage.setStartProcessingTime(System.currentTimeMillis());
      
      log.trace("New message [" + msg.getUniqueId() + "] ::: Queue slots available: " +  processingQueue.remainingCapacity());
      processingQueue.put(translatedMessage);
      
      // If we are expecting replies, lets block the consumer until we get some replies back.
      if (SendMode.single(this.getTargetSendMode())) {
        consumerQueue.put(translatedMessage);
      }
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
      return;
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
    
    if(this.getQueueCapacity() <= 0)
      throw new CoreException("Queue capacity must be greater than 0.");
    
    this.getClusteredEventBus().setConsumerEventListener(this);
    
    if(this.getVertXMessageTranslator() == null)
      this.setVertXMessageTranslator(new VertXMessageTranslator());
    
    processingQueue = new ArrayBlockingQueue<>(this.getQueueCapacity(), true);
    consumerQueue = new BlockingExpiryQueue<>(this.getQueueCapacity(), true);
    consumerQueue.setExpiryTimeout(this.getItemExpiryTimeout() != null ? this.getItemExpiryTimeout() : new TimeInterval(DEFAULT_ITEM_EXPIRY_SECONDS, TimeUnit.SECONDS));
    consumerQueue.registerExpiryListener(this);
    
    objectMetadataCache.clear();
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
        if (SendMode.single(this.getTargetSendMode())) {
          getClusteredEventBus().send(targetComponentId(xMessage), xMessage);
        } else {
          getClusteredEventBus().publish(targetComponentId(xMessage), xMessage);
        }
      } catch (InterlokException exception) {
        log.error("Cannot derive the target from the incoming message.", exception);
        try {
          this.handleBadMessage(this.getVertXMessageTranslator().translate(xMessage));
        } catch (CoreException e) {
          log.error("Cannot translate into AdaptrisMessage: " + xMessage);
        }
      }
    }
  }
  
  public void handleMessageReply(Message<Object> result) {
    VertXMessage resultMessage = (VertXMessage) result.body();
    
    AdaptrisMessage adaptrisMessage;
    try {
      adaptrisMessage = this.getVertXMessageTranslator().translate(resultMessage);
      moveObjectMetadata(adaptrisMessage);
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
        consumerQueue.remove(resultMessage); // unblock the consumer, now that we have completed a message. 
      }
      workflowEnd(adaptrisMessage, adaptrisMessage);
    }
  }

  private void moveObjectMetadata(AdaptrisMessage adaptrisMessage) {
    Map<Object, Object> cachedObjectMetadata = objectMetadataCache.get(adaptrisMessage.getUniqueId());
    if(cachedObjectMetadata != null) {
      for (Map.Entry<Object, Object> entry : cachedObjectMetadata.entrySet())
        adaptrisMessage.addObjectHeader(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void handle(Message<VertXMessage> event) {
    if(event.body() != null)
      this.onVertxMessage(event);
  }
  
  @Override
  public void itemExpired(VertXMessage item) {
    log.warn("Expecting message reply, but message has timed out: " + item);
    objectMetadataCache.remove(item.getAdaptrisMessage().getUniqueId());
  }
  
  @Override
  public boolean isEnabled(License license) {
    return license.isEnabled(LicenseType.Enterprise);
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

  MessageCodec<VertXMessage, VertXMessage> getMessageCodec() {
    return messageCodec;
  }

  void setMessageCodec(MessageCodec<VertXMessage, VertXMessage> messageCodec) {
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
    if(array != null) {
      for(VertXMessage message : array) {
        builder.append("\t" + message.getAdaptrisMessage().getUniqueId() + "\n");
      }
      log.trace(builder.toString());
    }
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

  public SendMode.Mode getTargetSendMode() {
    return targetSendMode;
  }

  public void setTargetSendMode(SendMode.Mode targetSendMode) {
    this.targetSendMode = targetSendMode;
  }

  public DataInputParameter<String> getTargetComponentId() {
    return targetComponentId;
  }

  public void setTargetComponentId(DataInputParameter<String> targetComponentId) {
    this.targetComponentId = targetComponentId;
  }

  ClusteredEventBus getClusteredEventBus() {
    return clusteredEventBus;
  }

  void setClusteredEventBus(ClusteredEventBus clusteredEventBus) {
    this.clusteredEventBus = clusteredEventBus;
  }

  ArrayBlockingQueue<VertXMessage> getProcessingQueue() {
    return processingQueue;
  }

  void setProcessingQueue(ArrayBlockingQueue<VertXMessage> processingQueue) {
    this.processingQueue = processingQueue;
  }

  public TimeInterval getItemExpiryTimeout() {
    return itemExpiryTimeout;
  }

  public void setItemExpiryTimeout(TimeInterval itemExpiryTimeout) {
    this.itemExpiryTimeout = itemExpiryTimeout;
  }

}
