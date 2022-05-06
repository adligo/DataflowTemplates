package com.google.cloud.teleport.util;

import java.util.Objects;

import org.apache.beam.sdk.options.ValueProvider;

/**
 * A simple value provider that can actually be used.
 * 
 * @author scott-@-adligo
 *
 * @param <T>
 */
public class SimpleValueProvider<T> implements ValueProvider<T> {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  public static final String A_VALUE_IS_REQUIRED = "A value is required!";
  private final T t;
  
  public SimpleValueProvider() {
    t = null;
  }

  public SimpleValueProvider(T t) {
    this.t = Objects.requireNonNull(t,A_VALUE_IS_REQUIRED);
  }
  
  @Override
  public T get() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isAccessible() {
    if (t == null) {
      return false;
    }
    return true;
  }

}
