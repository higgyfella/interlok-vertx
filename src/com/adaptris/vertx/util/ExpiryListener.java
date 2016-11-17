package com.adaptris.vertx.util;

public interface ExpiryListener<T> {

  public void itemExpired(T item);
  
}
