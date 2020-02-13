package com.adaptris.vertx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageListener;
import com.adaptris.core.CoreException;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandardWorkflowImpl;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.util.NumberUtils;
import com.adaptris.util.TimeInterval;
import com.adaptris.vertx.util.BlockingExpiryQueue;
import com.adaptris.vertx.util.ExpiryListener;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;

/**
 * A clustered workflow that allows you to farm out the service-list processing to a random instance of this workflow in your
 * cluster.
 * 
 * <p>
 * Since there are no guarantees about which instance will process the message, this workflow will not support the new
 * {@link AdaptrisMessageListener#onAdaptrisMessage(AdaptrisMessage, Consumer)} method; <strong>any callbacks requested by the
 * consumer will be ignored</strong>
 * </p>
 * 
 * <p>
 * Clusters are managed and discovered by Hazelcast. To create a cluster you simply need to have multiple instances of this workflow
 * either in different channels or different instances of Interlok with the same vertx-id on each instance of the workflow.It is
 * recommended that you explicitly configure {@link #setClusterId(String)}; if it is not expliclty configured, then we default to
 * {@link #getUniqueId()}.
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
 * @config clustered-workflow
 * @since 3.5.0
 * @author Aaron
 *
 */
@AdapterComponent
@ComponentProfile(summary = "A workflow that allows clustered processing of services.", tag = "workflow,clustering,vertx")
@XStreamAlias("clustered-workflow")
@DisplayOrder(order = {"clusterId", "continueOnError", "disableDefaultMessageCount", "sendEvents", "logPayload"})
public class VertxWorkflow extends StandardWorkflowImpl
    implements Handler<Message<VertXMessage>>, ConsumerEventListener, ExpiryListener<VertXMessage> {
  
  private static final int DEFAULT_MAX_THREADS = 10;
  
  private static final TimeInterval DEFAULT_ITEM_EXPIRY = new TimeInterval(30L, TimeUnit.SECONDS);
  
  private static final int DEFAULT_QUEUE_SIZE = 10;
  
  @InputFieldDefault(value = "10")
  private Integer queueCapacity;

  private String clusterId;

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

  @AdvancedConfig
  @Valid
  private TimeInterval itemExpiryTimeout;
  
  @AdvancedConfig
  @Valid
  private VertxProperties vertxProperties;
  
  private transient MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private transient ArrayBlockingQueue<VertXMessage> processingQueue;
  
  private transient BlockingExpiryQueue<VertXMessage> consumerQueue;
  
  private transient ExecutorService messageExecutor;
  
  private transient Future<?> messageExecutorHandle;
      
  private transient ClusteredEventBus clusteredEventBus;
  
  private transient Map<String, Map<Object, Object>> objectMetadataCache;
  
  private transient ConsumerLatch latch;
  
  private transient ExecutorService executorService;
  
  private Integer maxThreads;

  public VertxWorkflow() {
    super();
    setMessageCodec(new AdaptrisMessageCodec());
    messageExecutor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
    setTargetSendMode(SendMode.Mode.SINGLE);
    clusteredEventBus = new ClusteredEventBus();
    objectMetadataCache = new HashMap<>();
  }
  
  private void queueMessage(AdaptrisMessage msg) {
    try {
      workflowStart(msg);
      log.debug("start processing msg [{}]", msg);      
      objectMetadataCache.put(msg.getUniqueId(), msg.getObjectHeaders()); 
      
      VertXMessage translatedMessage = getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord());
      translatedMessage.setStartProcessingTime(System.currentTimeMillis());
      
      log.trace("New message [{}]::: Queue slots available: {}", msg.getUniqueId(), getProcessingQueue().remainingCapacity());
      getProcessingQueue().put(translatedMessage);
      
      // If we are expecting replies, lets block the consumer until we get some replies back.
      if (SendMode.single(getTargetSendMode())) {
        consumerQueue.put(translatedMessage);
      }
      log.trace("New queue size : {}", getProcessingQueue().remainingCapacity());
      reportQueue("new message put [" + msg.getUniqueId() + "]");
    } catch (CoreException e) {
      log.error("Error processing message: ", e);
      handleBadMessage(msg);
    } catch (InterruptedException e) {
      log.error("Error processing message: ", e);
      handleBadMessage(msg);
    }
  }
  

  @Override
  public void onAdaptrisMessage(AdaptrisMessage msg, Consumer<AdaptrisMessage> success) {
    // Since we ultimately change the message into one that's "serializable"
    // we won't add the callback.
    // ListenerCallbackHelper.prepare(msg, success);
    queueMessage(msg);
  }

  @Override
  protected void resubmitMessage(AdaptrisMessage msg) {
    queueMessage(msg);
  }

  public void onVertxMessage(Message<VertXMessage> xMessage) {
    AdaptrisMessage adaptrisMessage = null;
    VertXMessage vxMessage = null;
    try {
      vxMessage = xMessage.body();
      adaptrisMessage = getVertXMessageTranslator().translate(vxMessage);
      log.trace("Incoming message: {}", adaptrisMessage.getUniqueId());
    } catch (CoreException e) {
      log.error("Error translating incoming message.", e);
      return;
    }

    for(Service service : getServiceCollection()) {
      InterlokService interlokService = new InterlokService(service.getUniqueId());
      
      try {
        service.doService(adaptrisMessage);
        interlokService.setState(ServiceState.COMPLETE);
      } catch (ServiceException ex) {
        log.error("Error running service.", ex);
        interlokService.setState(ServiceState.ERROR);
        interlokService.setException(ex);
        if(!continueOnError()) {
          break;
        }
      } finally {
        vxMessage.getServiceRecord().addService(interlokService);
      }
    }
    
    try {
      VertXMessage vertXMessage = getVertXMessageTranslator().translate(adaptrisMessage);
      vxMessage.setAdaptrisMessage(vertXMessage.getAdaptrisMessage());
      xMessage.reply(vxMessage);
    } catch (CoreException e) {
      log.error("Could not translate the Vertx Message to an AdaptrisMessage", e);
    }    
  }

  @Override
  protected void initialiseWorkflow() throws CoreException {
    super.initialiseWorkflow();
    clusteredEventBus.setMessageCodec(getMessageCodec());
    
    this.setExecutorService(new ThreadPoolExecutor(1, maxThreads(), 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()));
    
    if (queueCapacity() <= 0) {
      throw new CoreException("Queue capacity must be greater than 0.");
    }

    if(getVertXMessageTranslator() == null) {
      setVertXMessageTranslator(new VertXMessageTranslator());
    }
    
    setProcessingQueue(new ArrayBlockingQueue<>(queueCapacity(), true));
    consumerQueue = new BlockingExpiryQueue<>(queueCapacity(), true);
    consumerQueue.setExpiryTimeout(itemExpiryTimeout());
    consumerQueue.registerExpiryListener(this);
    
    objectMetadataCache.clear();
  }

  @Override
  protected void prepareWorkflow() throws CoreException {
    super.prepareWorkflow();
  }

  @Override
  protected void startWorkflow() throws CoreException {
    super.startWorkflow();
    latch = ConsumerLatch.build();
    clusteredEventBus.startClusteredConsumer(this, this.getVertxProperties());
    latch.waitForComplete();
  }
  
  @Override
  public void consumerStarted() {
    latch.complete();
    Runnable messageProcessorRunnable = new Runnable() {
      @Override
      public void run() {
        boolean interrupted = false;
        while(!messageExecutorHandle.isDone() && !interrupted) {
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
    VertXMessage xMessage = getProcessingQueue().poll(1L, TimeUnit.SECONDS);
    
    if(xMessage != null) {
      reportQueue("after a get [" + xMessage.getAdaptrisMessage().getUniqueId() + "]");
      // send it to vertx   
      try {
        if (SendMode.single(getTargetSendMode())) {
          getClusteredEventBus().send(targetComponentId(xMessage), xMessage, true);
        } else {
          getClusteredEventBus().publish(targetComponentId(xMessage), xMessage);
        }
      } catch (InterlokException exception) {
        log.error("Cannot derive the target from the incoming message.", exception);
        try {
          this.handleBadMessage(getVertXMessageTranslator().translate(xMessage));
        } catch (CoreException e) {
          log.error("Cannot translate into AdaptrisMessage: " + xMessage);
        }
      }
    }
  }
  
  @Override
  public void handleMessageReply(Message<Object> result) {
    VertXMessage resultMessage = (VertXMessage) result.body();
    
    AdaptrisMessage adaptrisMessage;
    try {
      adaptrisMessage = getVertXMessageTranslator().translate(resultMessage);
      moveObjectMetadata(adaptrisMessage);
    } catch (CoreException e) {
      log.error("Cannot translate the reply message back to an AdaptrisMessage", e);
      return;
    }
    log.debug("Received reply: {}", resultMessage.getAdaptrisMessage().getUniqueId());
    log.trace("{}: Service record : {}", resultMessage.getAdaptrisMessage().getUniqueId(), resultMessage.getServiceRecord());
    
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
      } catch (Exception e) {
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
      for (Map.Entry<Object, Object> entry : cachedObjectMetadata.entrySet()) {
        adaptrisMessage.addObjectHeader(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void handle(Message<VertXMessage> event) {
    this.getExecutorService().submit(new Runnable() {
      
      @Override
      public void run() {
        onVertxMessage(event);
      }
    });
  }
  
  @Override
  public void itemExpired(VertXMessage item) {
    log.warn("Expecting message reply, but message has timed out: {}", item);
    objectMetadataCache.remove(item.getAdaptrisMessage().getUniqueId());
  }
  
  @Override
  protected void stopWorkflow() {
    super.stopWorkflow();
    if(messageExecutorHandle != null) {
      messageExecutorHandle.cancel(false);
    }
    if(getClusteredEventBus().getEventBus() != null) {
      getClusteredEventBus().getEventBus().consumer(getUniqueId()).unregister();
    }
  }
  
  @Override
  protected void closeWorkflow() {
    super.closeWorkflow();
    ManagedThreadFactory.shutdownQuietly(this.getExecutorService(), 30000l);
    
    if(messageExecutorHandle != null) {
      if(!messageExecutorHandle.isCancelled()) {
        messageExecutorHandle.cancel(true);
      }
    }
  }

  @Override
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Sets the ID that will be registered with vertx.
   * 
   * @param vertxId if not configured, defaults to {@link #getUniqueId()}
   */
  public void setClusterId(String vertxId) {
    clusterId = vertxId;
  }

  int queueCapacity() {
    return getQueueCapacity() != null ? getQueueCapacity().intValue() : DEFAULT_QUEUE_SIZE;
  }

  public Integer getQueueCapacity() {
    return queueCapacity;
  }

  public void setQueueCapacity(Integer queueCapacity) {
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
    AdaptrisMessage adaptrisMessage = getVertXMessageTranslator().translate(vertxMessage);
    return getTargetComponentId().extract(adaptrisMessage);
  }

  private void reportQueue(String title) {
    if (log.isTraceEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("\nCurrent Queue State (" + title + "):\n");

      VertXMessage[] array = new VertXMessage[getProcessingQueue().size()];
      array = getProcessingQueue().toArray(array);
      if (array != null) {
        for (VertXMessage message : array) {
          builder.append("\t" + message.getAdaptrisMessage().getUniqueId() + "\n");
        }
        log.trace(builder.toString());
      }
    }
  }

  protected boolean continueOnError() {
    return getContinueOnError() != null ? getContinueOnError() : false;
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

  TimeInterval itemExpiryTimeout() {
    return getItemExpiryTimeout() != null ? getItemExpiryTimeout() : DEFAULT_ITEM_EXPIRY;
  }

  ExecutorService getExecutorService() {
    return executorService;
  }

  void setExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }
  
  protected int maxThreads() {
    return NumberUtils.toIntDefaultIfNull(getMaxThreads(), DEFAULT_MAX_THREADS);
  }

  public Integer getMaxThreads() {
    return maxThreads;
  }

  public void setMaxThreads(Integer maxThreads) {
    this.maxThreads = maxThreads;
  }

  public VertxProperties getVertxProperties() {
    return vertxProperties;
  }

  public void setVertxProperties(VertxProperties vertxProperties) {
    this.vertxProperties = vertxProperties;
  }

}
