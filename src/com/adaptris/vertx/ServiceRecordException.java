package com.adaptris.vertx;

public class ServiceRecordException extends Exception {

  private static final long serialVersionUID = 3635083785426779956L;
  
  /**
   * <p>
   * Creates a new instance.
   * </p>
   */
  public ServiceRecordException() {
    super();
  }

  /**
   * <p>
   * Creates a new instance with a reference to a previous 
   * <code>Exception</code>.
   * </p>
   * @param cause a previous, causal <code>Exception</code>
   */
  public ServiceRecordException(Throwable cause) {
    super(cause);
  }

  /**
   * <p>
   * Creates a new instance with a description of the <code>Exception</code>.
   * </p>
   * @param description description of the <code>Exception</code>
   */
  public ServiceRecordException(String description) {
    super(description);
  }

  /**
   * <p>
   * Creates a new instance with a reference to a previous 
   * <code>Exception</code> and a description of the <code>Exception</code>.
   * </p>
   * @param description of the <code>Exception</code>
   * @param cause previous <code>Exception</code>
   */
  public ServiceRecordException(String description, Throwable cause) {
    super(description, cause);
  }

}
