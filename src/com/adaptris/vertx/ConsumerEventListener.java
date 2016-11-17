package com.adaptris.vertx;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;

public interface ConsumerEventListener extends Handler<Message<VertXMessage>>{

  public void consumerStarted();
  
  public void handleMessageReply(Message<Object> result);
  
}
