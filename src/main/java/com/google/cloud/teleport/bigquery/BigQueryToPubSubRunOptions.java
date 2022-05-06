package com.google.cloud.teleport.bigquery;


import org.apache.beam.sdk.options.PipelineOptions;
/**
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

}
