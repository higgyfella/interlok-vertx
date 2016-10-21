package com.adaptris.vertx;

import org.apache.commons.lang.StringUtils;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.licensing.License;
import com.adaptris.core.licensing.License.LicenseType;
import com.adaptris.core.licensing.LicenseChecker;
import com.adaptris.core.licensing.LicensedComponent;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;

@XStreamAlias("vertx-service")
public class VertxService extends ServiceImp implements Handler<Message<VertXMessage>>, ConsumerEventListener, LicensedComponent {
  
  private enum SEND_MODE {
    ALL,
    SINGLE;
  }
  
  private static final String DEFAULT_SEND_MODE = SEND_MODE.SINGLE.name();
  
  private Service service;
  
  private Service replyService;
  
  private VertXMessageTranslator vertXMessageTranslator;
  
  private DataInputParameter<String> targetComponentId;
  
  private String targetSendMode;
  
  private transient MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private transient ClusteredEventBus clusteredEventBus;
  
  public VertxService() {
    super();
    this.setMessageCodec(new AdaptrisMessageCodec());
    this.setTargetSendMode(DEFAULT_SEND_MODE);
    this.setTargetComponentId(new ConstantDataInputParameter());
    clusteredEventBus = new ClusteredEventBus();
    clusteredEventBus.setMessageCodec(getMessageCodec());
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord());
      translatedMessage.setStartProcessingTime(System.currentTimeMillis());
      
      
      if(!StringUtils.isEmpty(this.getTargetComponentId().extract(msg))) {
        try {
          if(this.getTargetSendMode().equalsIgnoreCase(SEND_MODE.SINGLE.name())) {
            getClusteredEventBus().send(getTargetComponentId().extract(msg), translatedMessage);
          } else {
            getClusteredEventBus().publish(getTargetComponentId().extract(msg), translatedMessage);
          }
        } catch (InterlokException exception) {
          log.error("Cannot derive the target from the incoming message.", exception);
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
      log.error("Cannot translate the reply message back to an AdaptrisMessage", e);
      return;
    }
    log.debug("Received reply: " + resultMessage.getAdaptrisMessage().getUniqueId());
    
    if(this.getReplyService() != null) {
      try {
        this.getReplyService().doService(adaptrisMessage);
      } catch (ServiceException e) {
        log.error("Unable to process service reply.", e);
      }
    }
  }

  @Override
  public void prepare() throws CoreException {
    LicenseChecker.newChecker().checkLicense(this);
    this.getService().prepare();
  }

  @Override
  protected void initService() throws CoreException {
    LifecycleHelper.init(this.getService());
    
    if(this.getVertXMessageTranslator() == null)
      this.setVertXMessageTranslator(new VertXMessageTranslator());
  }
  
  public void start() throws CoreException {
    clusteredEventBus.startClusteredConsumer(this.getUniqueId());
    
    LifecycleHelper.start(this.getService());
  }
  
  @Override
  public void consumerStarted() {
  }

  @Override
  protected void closeService() {
    LifecycleHelper.close(this.getService());
  }

  public Service getService() {
    return service;
  }

  public void setService(Service service) {
    this.service = service;
  }

  @Override
  public boolean isEnabled(License license) {
    return license.isEnabled(LicenseType.Standard);
  }

  private VertXMessage onVertxMessage(VertXMessage vxMessage) {
    AdaptrisMessage adaptrisMessage = null;
    try {
      adaptrisMessage = this.getVertXMessageTranslator().translate(vxMessage);
      log.trace("Incoming message: " + adaptrisMessage.getUniqueId());
    } catch (CoreException e) {
      log.error("Error translating incoming message.", e);
      return null;
    }

    Service service = this.getService();
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
  
    return vxMessage;
  }
  
  @Override
  public void handle(Message<VertXMessage> event) {
    VertXMessage vertXMessage = this.onVertxMessage(event.body());
    event.reply(vertXMessage);
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

  public DataInputParameter<String> getTargetComponentId() {
    return targetComponentId;
  }

  public void setTargetComponentId(DataInputParameter<String> targetComponentId) {
    this.targetComponentId = targetComponentId;
  }

  public String getTargetSendMode() {
    return targetSendMode;
  }

  public void setTargetSendMode(String targetSendMode) {
    this.targetSendMode = targetSendMode;
  }

  public Service getReplyService() {
    return replyService;
  }

  public void setReplyService(Service replyService) {
    this.replyService = replyService;
  }

  public ClusteredEventBus getClusteredEventBus() {
    return clusteredEventBus;
  }

  public void setClusteredEventBus(ClusteredEventBus clusteredEventBus) {
    this.clusteredEventBus = clusteredEventBus;
  }

  
}
