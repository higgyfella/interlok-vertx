package com.adaptris.vertx;

import static com.adaptris.core.util.ServiceUtil.discardNulls;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisComponent;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.NullProcessingExceptionHandler;
import com.adaptris.core.ProcessingExceptionHandler;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.ServiceWrapper;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.adaptris.util.NumberUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;


/**
 * <p>
 * A clustered service that allows you to farm out the service processing to a random instance of this service in your cluster.<br/>
 * Clusters are managed and discovered by Hazelcast. To create a cluster you simply need to have multiple instances of this service
 * either in different workflows or different instances of Interlok with the same vertx-id on each instance of the service. It is
 * recommended that you explicitly configure {@link #setClusterId(String)}; if it is not expliclty configured, then we default to
 * {@link #getUniqueId()}.
 * </p>
 * <p>
 * There are two modes of clustering; "all" and "single" (default), configured with the target-send-mode option.<br/>
 * If you select "all", then each message will be sent to ALL instances in the cluster. Likewise if you select "single" then a
 * single random service instance will process the message.<br/>
 * Additionally if you choose "single" once the service instance has finished running the original service will receive the
 * processed message as a reply.
 * </p>
 * <p>
 * When an instance of this service receives a message to process, it will run the wrapped-service (which may also be a
 * service-list). But no further services in the workflow will be run.
 * </p>
 * <p>
 * Should you choose to send each message to only one instance in the cluster then the original service will receive a reply and run
 * the wrapped reply-service (which may also be a service-list). <br/>
 * And should the service(s) fail you can configure a {@link ProcessingExceptionHandler}.
 * </p>
 * <p>
 * Do note however, that any reply expected will not be waited for.<br/>
 * If there are services to be run after this service in the originally consumed workflow, they will run without waiting for the
 * reply.
 * </p>
 * <p>
 * You can choose the cluster to send messages to by configuring the target-component-id. The value of which will match the cluster
 * name (unique-id) of any clustered service.<br/>
 * Any message will be sent to the named cluster, which may also include this very instance of the service if this service shares
 * the same unique-id as the other clustered service instances.
 * </p>
 * 
 * @config clustered-service
 * @since 3.5.0
 * @author Aaron
 *
 */
@AdapterComponent
@ComponentProfile(summary = "Allows clustered single service processing.", tag = "service,clustering,vertx")
@XStreamAlias("clustered-service")
@DisplayOrder(order = {"clusterId", "targetSendMode"})
public class VertxService extends ServiceImp implements Handler<Message<VertXMessage>>, ConsumerEventListener, ServiceWrapper {
  
  private static final int DEFAULT_MAX_THREADS = 10;
  
  private String clusterId;
  
  @Valid
  private Service service;
  
  @Valid
  private Service replyService;
  
  @Valid
  private DataInputParameter<String> targetComponentId;
  
  @NotNull
  @AutoPopulated
  private SendMode.Mode targetSendMode;

  @AdvancedConfig
  @Valid
  private VertXMessageTranslator vertXMessageTranslator;
  
  @AdvancedConfig
  @Valid
  private ProcessingExceptionHandler replyServiceExceptionHandler;
  
  @AdvancedConfig
  @Valid
  private VertxProperties vertxProperties;
  
  private transient MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private transient ClusteredEventBus clusteredEventBus;
  private transient ConsumerLatch latch;
  
  private transient ExecutorService executorService;
  
  private Integer maxThreads;
  
  public VertxService() {
    super();
    this.setMessageCodec(new AdaptrisMessageCodec());
    this.setTargetSendMode(SendMode.Mode.SINGLE);
    this.setTargetComponentId(new ConstantDataInputParameter());
    this.setClusteredEventBus(new ClusteredEventBus());
    this.getClusteredEventBus().setMessageCodec(getMessageCodec());
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord());
      translatedMessage.setStartProcessingTime(System.currentTimeMillis());
      
      if((this.getTargetComponentId() != null) && (!StringUtils.isEmpty(this.getTargetComponentId().extract(msg)))) {
        if (SendMode.single(this.getTargetSendMode())) {
          getClusteredEventBus().send(getTargetComponentId().extract(msg), translatedMessage, getReplyService() != null);
        } else {
          getClusteredEventBus().publish(getTargetComponentId().extract(msg), translatedMessage);
        }
      } else {
        this.onVertxMessage(translatedMessage);
      }
    } catch (InterlokException e) {
      throw new ServiceException(e);
    }
  }

  public void handleMessageReply(Message<Object> result) {
    VertXMessage resultMessage = (VertXMessage) result.body();
    
    AdaptrisMessage adaptrisMessage;
    try {
      adaptrisMessage = this.getVertXMessageTranslator().translate(resultMessage);
    } catch (CoreException e) {
      log.error("Cannot translate the reply message back to an AdaptrisMessage: {}", resultMessage, e);
      return;
    }
    log.debug("Received reply: {}", resultMessage.getAdaptrisMessage().getUniqueId());
    log.trace("{}: Service record : {}", resultMessage.getAdaptrisMessage().getUniqueId(), resultMessage.getServiceRecord());

    if(this.getReplyService() != null) {
      try {
        this.getReplyService().doService(adaptrisMessage);
      } catch (ServiceException e) {
        log.error("Unable to process service reply: {}", resultMessage, e);
        replyExceptionHandler().handleProcessingException(adaptrisMessage);
      }
    }
  }

  @Override
  public void prepare() throws CoreException {
    prepare(getService());
    prepare(getReplyService());
    prepare(getReplyServiceExceptionHandler());
  }

  private void prepare(AdaptrisComponent c) throws CoreException {
    if (c != null) {
      c.prepare();
    }
  }

  @Override
  protected void initService() throws CoreException {
    if (this.getVertXMessageTranslator() == null) this.setVertXMessageTranslator(new VertXMessageTranslator());
    
    this.setExecutorService(new ThreadPoolExecutor(1, maxThreads(), 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>()));
    clusteredEventBus.setMessageCodec(getMessageCodec());
    LifecycleHelper.init(this.getService());
    LifecycleHelper.init(this.getReplyService());
    LifecycleHelper.init(this.getReplyServiceExceptionHandler());
  }
  
  @Override
  public void start() throws CoreException {
    LifecycleHelper.start(this.getService());
    LifecycleHelper.start(this.getReplyService());
    LifecycleHelper.start(this.getReplyServiceExceptionHandler());
    latch = ConsumerLatch.build();
    clusteredEventBus.startClusteredConsumer(this, this.getVertxProperties());
    latch.waitForComplete();
  }
  
  @Override
  public void stop() {
    LifecycleHelper.stop(this.getService());
    LifecycleHelper.stop(this.getReplyService());
    LifecycleHelper.stop(this.getReplyServiceExceptionHandler());
  }

  @Override
  public void consumerStarted() {
    latch.complete();
  }

  @Override
  protected void closeService() {
    LifecycleHelper.close(this.getService());
    LifecycleHelper.close(this.getReplyService());
    LifecycleHelper.close(this.getReplyServiceExceptionHandler());
    
    ManagedThreadFactory.shutdownQuietly(this.getExecutorService(), 30000l);
  }

  public Service getService() {
    return service;
  }

  public void setService(Service service) {
    this.service = service;
  }

  private VertXMessage onVertxMessage(VertXMessage vxMessage) {
    AdaptrisMessage adaptrisMessage = null;
    try {
      adaptrisMessage = this.getVertXMessageTranslator().translate(vxMessage);
      log.debug("Incoming message: {} being handled by {}", adaptrisMessage.getUniqueId(), LoggingHelper.friendlyName(this));
    } catch (CoreException e) {
      log.error("Error translating incoming message.", e);
      return null;
    }

    Service service = this.getService();
    if (service != null) {
      InterlokService interlokService = new InterlokService(service.getUniqueId());

      try {
        service.doService(adaptrisMessage);
        interlokService.setState(ServiceState.COMPLETE);
        VertXMessage vertXMessage = this.getVertXMessageTranslator().translate(adaptrisMessage);
        vxMessage.setAdaptrisMessage(vertXMessage.getAdaptrisMessage());
      } catch (CoreException ex) {
        log.error("Error running service.", ex);
        interlokService.setState(ServiceState.ERROR);
        interlokService.setException(ex);
      } finally {
        vxMessage.getServiceRecord().addService(interlokService);
      }
    } else {
      log.warn("No service configured for the vertx-service ({}), not processing", LoggingHelper.friendlyName(this));
    }
    return vxMessage;
  }
  
  @Override
  public void handle(Message<VertXMessage> event) {
    this.getExecutorService().submit(new Runnable() {
      
      @Override
      public void run() {
        VertXMessage vertXMessage = onVertxMessage(event.body());
        event.reply(vertXMessage);
      }
    });
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

  public DataInputParameter<String> getTargetComponentId() {
    return targetComponentId;
  }

  public void setTargetComponentId(DataInputParameter<String> targetComponentId) {
    this.targetComponentId = targetComponentId;
  }

  public SendMode.Mode getTargetSendMode() {
    return targetSendMode;
  }

  public void setTargetSendMode(SendMode.Mode targetSendMode) {
    this.targetSendMode = targetSendMode;
  }

  public Service getReplyService() {
    return replyService;
  }

  public void setReplyService(Service replyService) {
    this.replyService = replyService;
  }

  ClusteredEventBus getClusteredEventBus() {
    return clusteredEventBus;
  }

  void setClusteredEventBus(ClusteredEventBus clusteredEventBus) {
    this.clusteredEventBus = clusteredEventBus;
  }

  public ProcessingExceptionHandler getReplyServiceExceptionHandler() {
    return replyServiceExceptionHandler;
  }

  public void setReplyServiceExceptionHandler(ProcessingExceptionHandler replyServiceExcecptionHandler) {
    this.replyServiceExceptionHandler = replyServiceExcecptionHandler;
  }

  ProcessingExceptionHandler replyExceptionHandler() {
    return getReplyServiceExceptionHandler() != null ? getReplyServiceExceptionHandler() : new NoOpExceptionHandler();
  }

  public String getClusterId() {
    return clusterId;
  }

  /**
   * Sets the ID that will be registered with vertx.
   * 
   * @param vertxId if not configured, defaults to {@link #getUniqueId()}
   */
  public void setClusterId(String vertxId) {
    this.clusterId = vertxId;
  }

  @Override
  public Service[] wrappedServices() {
    return discardNulls(getService(), getReplyService());
  }

  // Completely no-op
  private class NoOpExceptionHandler extends NullProcessingExceptionHandler {

    public void handleProcessingException(AdaptrisMessage msg) {
    }
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
