package com.google.cloud.teleport.bigquery;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.http.util.CharArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.teleport.util.SimpleValueProvider;
/**
 * This interface 
 * For use with code liek this; </br>
 * 
 * <pre><code>
 * PipelineOptionsFactory.fromArgs(args)
 *   .withValidation()
 *   .as(BigQueryToPubSubRunOptions.class);
 *</code></pre?>
 * 
 * @author scott
 *
 */
public interface BigQueryToPubSubRunOptions extends PipelineOptions {
  
  /**
   * The GCP project id to use when running the input query
   * @return
   */
  @Description("The GCP project id to use when running the BigQuery.")
  @Required
  ValueProvider<String> getInputProjectId();
  
  /**
   * The big query that creates the input for the BigQueryToPubSub job.
   * @return
   */
  @Description("The query to run.")
  ValueProvider<String> getInputQuery();

  /**
   * The file path of a file containing a input query
   * @return
   */
  @Description("The system dependent file path to a file that contains the query, and if present will be used to replace the inputQuery's value.")
  @Required
  ValueProvider<String> getInputQueryFilePath();
  
  /**
   * Note Googles annotation has bugs, it doesn't allow method chaning 
   * so I can't return this.
   * @see {@link BigQueryToPubSubRunOptions#getInputQuery()}
   * @param query
   */
  void setInputQuery(ValueProvider<String> query);
  
  /**
   * Note Googles annotation has bugs, it doesn't allow method chaning 
   * so I can't return this.
   * @see {@link BigQueryToPubSubRunOptions#getInputQueryFilePath()}
   * @param query
   */
  void setInputQueryFilePath(ValueProvider<String> query);

  /**
   * Note Googles annotation has bugs, it doesn't allow method chaning 
   * so I can't return this.
   * @see {@link BigQueryToPubSubRunOptions#getInputProjectId()}
   * @param query
   */
  void setInputProjectId(ValueProvider<String> projectId);

}
