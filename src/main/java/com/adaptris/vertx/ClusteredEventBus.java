package com.adaptris.vertx;

import static org.apache.commons.lang.StringUtils.isEmpty;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;

class ClusteredEventBus {

  private transient ConsumerEventListener consumerEventListener;
  
  private transient MessageCodec<VertXMessage, VertXMessage> messageCodec;
  
  private transient Vertx vertX;
  
  private transient EventBus eventBus;
    
  public void startClusteredConsumer(ConsumerEventListener listener, VertxProperties vertxOptions) {
    setConsumerEventListener(listener);
    Vertx.clusteredVertx(new VertxPropertyBuilder().build(vertxOptions), new Handler<AsyncResult<Vertx>>() {
      @Override
      public void handle(AsyncResult<Vertx> event) {
        vertX = event.result();
        eventBus = vertX.eventBus();
        eventBus.registerDefaultCodec(VertXMessage.class, getMessageCodec());
        eventBus.consumer(vertxId(listener), getConsumerEventListener());
        
        getConsumerEventListener().consumerStarted();
      }
    });
  }
  
  public void send(String targetConsumer, Object message, boolean expectReply) {
    if (expectReply) {
      this.getEventBus().request(targetConsumer, message, replyHandler -> {
        if (replyHandler.succeeded()) {
          getConsumerEventListener().handleMessageReply(replyHandler.result());
        }
      });
    }
    else {
      this.getEventBus().send(targetConsumer, message);
    }
  }
  
  public void publish(String targetConsumer, Object message) {
    this.getEventBus().publish(targetConsumer, message);
  }
  
  private ConsumerEventListener getConsumerEventListener() {
    return consumerEventListener;
  }

  private void setConsumerEventListener(ConsumerEventListener consumerEventListener) {
    this.consumerEventListener = consumerEventListener;
  }

  public MessageCodec<VertXMessage, VertXMessage> getMessageCodec() {
    return messageCodec;
  }

  public void setMessageCodec(MessageCodec<VertXMessage, VertXMessage> messageCodec) {
    this.messageCodec = messageCodec;
  }

  public EventBus getEventBus() {
    return eventBus;
  }

  public void setEventBus(EventBus eventBus) {
    this.eventBus = eventBus;
  }
  
  String vertxId(ConsumerEventListener c) {
    return !isEmpty(c.getClusterId()) ? c.getClusterId() : c.getUniqueId();
  }

}
