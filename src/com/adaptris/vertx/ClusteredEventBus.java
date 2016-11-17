package com.adaptris.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;

public class ClusteredEventBus {

  private ConsumerEventListener consumerEventListener;
  
  private MessageCodec<VertXMessage, VertXMessage> messageCodec; 
  
  private transient Vertx vertX;
  
  private transient EventBus eventBus;
  
  public void startClusteredConsumer(String endpointName) {
    Vertx.clusteredVertx(new VertxOptions(), new Handler<AsyncResult<Vertx>>() {
      @Override
      public void handle(AsyncResult<Vertx> event) {
        vertX = event.result();
        eventBus = vertX.eventBus();
        eventBus.registerDefaultCodec(VertXMessage.class, getMessageCodec());
        eventBus.consumer(endpointName, getConsumerEventListener());
        
        getConsumerEventListener().consumerStarted();
      }
    });
  }
  
  public void send(String targetConsumer, Object message) {
    this.getEventBus().send(targetConsumer, message, replyHandler -> {
      if (replyHandler.succeeded()) {
        getConsumerEventListener().handleMessageReply(replyHandler.result());
      }
    });
  }
  
  public void publish(String targetConsumer, Object message) {
    this.getEventBus().send(targetConsumer, message);
  }
  

  public ConsumerEventListener getConsumerEventListener() {
    return consumerEventListener;
  }

  public void setConsumerEventListener(ConsumerEventListener consumerEventListener) {
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
  
}
