package com.google.cloud.teleport.bigquery;


import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

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
public interface BigQueryToPubSubRunOptions extends PipelineOptions, GcpOptions {
  public static final String THE_GCP_TEMPLATE_LOACTION_IS_NOW = "The GcpTemplateLoaction is now ";
  public static final String THE_QUERY_VALUE_PROVIDER_IS = "The query value provider is ";
  static final Logger LOG = LoggerFactory.getLogger(BigQueryToPubSubRunOptions.class);
  
  @Description("The gcp template location.")
  ValueProvider<String> getGcpTemplateLocation();
  
  /**
   * @see {@link #getGcpTemplateLocation()}
   * @return
   */
  default String getGcpTemplateLocationString() {
    ValueProvider<String> vp = getGcpTemplateLocation();
    if (vp == null) {
      return null;
    }
    return vp.get();
  }
  
  /**
   * The big query that creates the input for the BigQueryToPubSub job.
   * @return
   */
  @Description("The query to run.")
  ValueProvider<String> getInputQuery();

  /**
   * @see {@link #getInputQuery()}
   * @return
   */
  default String getInputQueryString() {
    ValueProvider<String> vp = getInputQuery();
    if (vp == null) {
      return null;
    }
    return vp.get();
  }

  /**
   * The file path of a file containing a input query
   * @return
   */
  @Description("The system dependent file path to a file that contains the query, and if present will be used to replace the inputQuery's value.")
  @Required
  ValueProvider<String> getInputQueryFilePath();

  @Description("The gcp template location.")
  void setGcpTemplateLocation(ValueProvider<String> templateLocation);
  
  /**
   * Adapts a String to a ValueProvider
   * @see {@link #setGcpTemplateLocation(ValueProvider)}
   * @param query
   */
  default void setGcpTemplateLocationString(String location) {
    setGcpTemplateLocationString( location, LOG);
  }

  /**
   * Adapts a String to a ValueProvider
   * @see {@link #setGcpTemplateLocation(ValueProvider)}
   * @param query
   */
  default void setGcpTemplateLocationString(String location, Logger log) {
    ValueProvider<String> vp = ValueProvider.StaticValueProvider.of(location);
    if (log.isDebugEnabled()) {
      log.debug(THE_GCP_TEMPLATE_LOACTION_IS_NOW + vp);
    }
    setGcpTemplateLocation( vp);
  }
  
  /**
   * Note Googles annotation has bugs, it doesn't allow method chaning 
   * so I can't return this.
   * @see {@link BigQueryToPubSubRunOptions#getInputQuery()}
   * @param query
   */
  void setInputQuery(ValueProvider<String> query);
  
  /**
   * Adapts a String to a ValueProvider
   * @see {@link #setInputQuery(ValueProvider)}
   * @param query
   */
  default void setInputQueryString(String query) {
    setInputQueryString(query, LOG);
  }
  
  /**
   * Adapts a String to a ValueProvider
   * @see {@link #setInputQuery(ValueProvider)}
   * @param query
   */
  default void setInputQueryString(String query, Logger log) {
    ValueProvider<String> vp = ValueProvider.StaticValueProvider.of(query);
    if (log.isDebugEnabled()) {
      log.debug(THE_QUERY_VALUE_PROVIDER_IS + vp);
    }
    setInputQuery( vp);
  }
  
  /**
   * Note Googles annotation has bugs, it doesn't allow method chaning 
   * so I can't return this.
   * @see {@link BigQueryToPubSubRunOptions#getInputQueryFilePath()}
   * @param query
   */
  void setInputQueryFilePath(ValueProvider<String> query);

}
