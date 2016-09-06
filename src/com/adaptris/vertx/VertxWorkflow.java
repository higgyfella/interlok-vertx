package com.adaptris.vertx;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.StandardWorkflow;
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
public class VertxWorkflow extends StandardWorkflow implements Handler<Message<VertXMessage>> {
  
  private static final int DEFAULT_QUEUE_SIZE = 100;
  
  private int queueCapacity;
  
  private VertXMessageTranslator vertXMessageTranslator;
  
  private MessageCodec<?, ?> messageCodec;
  
  private transient ArrayBlockingQueue<VertXMessage> processingQueue;
  
  private transient ExecutorService messageExecutor;
  
  private transient Future<?> messageExecutorHandle;
  
  private transient Vertx vertX;
  
  private transient EventBus eventBus;
  
  private transient VertxWorkflow handler;

  public VertxWorkflow() {
    super();
    this.setQueueCapacity(DEFAULT_QUEUE_SIZE);
    this.setMessageCodec(new AdaptrisMessageCodec());
    processingQueue = new ArrayBlockingQueue<>(this.getQueueCapacity(), false);
    messageExecutor = Executors.newSingleThreadExecutor(new ManagedThreadFactory());
    handler = this;
  }
  
  @Override
  public void onAdaptrisMessage(AdaptrisMessage msg) {
    try {
      VertXMessage translatedMessage = this.getVertXMessageTranslator().translate(msg);
      translatedMessage.setServiceRecord(new ServiceRecord(this.getServiceCollection()));
      
      processingQueue.put(translatedMessage);
    } catch (CoreException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public void onVertxMessage(Message<VertXMessage> xMessage) {
    
  }

  @Override
  protected void initialiseWorkflow() throws CoreException {
    super.initialiseWorkflow();
    
    if(this.getVertXMessageTranslator() == null)
      this.setVertXMessageTranslator(new VertXMessageTranslator());
  }

  @Override
  protected void prepareWorkflow() throws CoreException {
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
      }
    });
    
    
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

  private void processQueuedMessage() throws InterruptedException {
    VertXMessage xMessage = processingQueue.poll(1L, TimeUnit.SECONDS);
    
    // send it to vertx
    try {
      eventBus.send(this.getUniqueId(), this.getVertXMessageTranslator().toVertxMessage(xMessage));
    } catch (CoreException e) {
      e.printStackTrace();
    }
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

  @Override
  public void handle(Message<VertXMessage> event) {
    this.onVertxMessage(event);
  }

  public MessageCodec<?, ?> getMessageCodec() {
    return messageCodec;
  }

  public void setMessageCodec(MessageCodec<?, ?> messageCodec) {
    this.messageCodec = messageCodec;
  }

}
