package com.google.cloud.teleport.util;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;

/**
 * A Delegating PTransform that delegates to some other PTransform.  In addition 
 * this logs to a Slf4j logger, so users can easily see what parts of a pipeline are
 * being executed when, so that when a problem occurs, it can be more easily identified.
 * 
 * @author scott-@-adligo
 *
 * @param <InputT>
 * @param <OutputT>
 */
public class DelegatingPTransform<InputT extends PInput, OutputT extends POutput> extends PTransform<InputT, OutputT> {
  public static final String A_DELEGATE_IS_REQUIRED = "A delegate is required!";
  public static final String A_LOG_IS_REQUIRED = "A log is required!";
  
  public static final String IN_POPULATE_DISPLAY_DATA_WITH = " in populateDisplayData with ";
  public static final String IN_SET_RESOURCE_HINGS_WITH = " in setResourceHings with;\n\t";
  public static final String IN_TO_STRING = " in toString";
  public static final String IN_VALIDATE_WITH = " in validate with;\n\t";
  public static final String IN_HASH_CODE = " in hashCode";
  public static final String IN_EXPAND_WITH_INPUT = " in expand with input;\n\t";
  public static final String IN_EQUALS = " in equals";
  public static final String IN_GET_ADDITIONAL_INPUTS = " in getAdditionalInputs";
  public static final String IN_GET_DEFAULT_OUTPUT_CODER = " in getDefaultOutputCoder";
  public static final String IN_GET_NAME = " in getName";
  public static final String IN_GET_RESOURCE_HINTS = " in getResourceHints";

  public static final String OPTIONS_ARE_REQUIRED = "Options are required!";
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final PTransform<InputT, OutputT> delegate;
  private final Logger log;
  
  public DelegatingPTransform(PTransform<? extends InputT, ? extends OutputT> delegate) {
    this(delegate, LoggerFactory.getLogger(DelegatingPTransform.class));
  }
  
  @SuppressWarnings("unchecked")
  public DelegatingPTransform(PTransform<? extends InputT,? extends  OutputT> delegate, Logger log) {
    this.delegate = (PTransform<InputT, OutputT>) Objects.requireNonNull(delegate, A_DELEGATE_IS_REQUIRED);
    this.log = Objects.requireNonNull(log, A_LOG_IS_REQUIRED);
  }

  public boolean equals(Object obj) {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_EQUALS);
    }
    return delegate.equals(obj);
  }

  public OutputT expand(InputT input) {
    if (log.isInfoEnabled()) {
      log.debug(super.getName() + IN_EXPAND_WITH_INPUT + input);
    }
    return delegate.expand(input);
  }

  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_GET_ADDITIONAL_INPUTS);
    }
    return delegate.getAdditionalInputs();
  }

  @SuppressWarnings("deprecation")
  public <T> Coder<T> getDefaultOutputCoder(InputT input, PCollection<T> output) throws CannotProvideCoderException {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_GET_DEFAULT_OUTPUT_CODER);
    }
    
    return delegate.getDefaultOutputCoder(input, output);
  }

  public String getName() {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_GET_NAME);
    }
    return delegate.getName();
  }

  public ResourceHints getResourceHints() {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_GET_RESOURCE_HINTS);
    }
    return delegate.getResourceHints();
  }
  
  public int hashCode() {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_HASH_CODE);
    }
    return delegate.hashCode();
  }

  public void populateDisplayData(Builder builder) {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_POPULATE_DISPLAY_DATA_WITH + builder);
    }
    delegate.populateDisplayData(builder);
  }

  public PTransform<InputT,OutputT> setResourceHints(@NonNull ResourceHints resourceHints) {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_SET_RESOURCE_HINGS_WITH + resourceHints);
    }
    return delegate.setResourceHints(resourceHints);
  }

  public String toString() {
    if (log.isDebugEnabled()) {
      log.debug(super.getName() + IN_TO_STRING);
    }
    return delegate.toString();
  }

  public void validate(@Nullable PipelineOptions options) {
    if (log.isInfoEnabled()) {
      log.debug(super.getName() + IN_VALIDATE_WITH + options);
    }
    delegate.validate(options);
  }
}
