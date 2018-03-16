package com.adaptris.vertx;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;

public interface ConsumerEventListener extends Handler<Message<VertXMessage>>{

  void consumerStarted();
  
  void handleMessageReply(Message<Object> result);
  
  String getClusterId();

  String getUniqueId();

}
