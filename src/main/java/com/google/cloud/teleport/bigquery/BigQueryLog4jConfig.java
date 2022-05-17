package com.google.cloud.teleport.bigquery;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.cloud.teleport.util.DelegatingPTransform;

/**
 * This class simply configures any number of logging levels progmatically,
 * by obtaining the Log4j logging implementaion and setting the level.
 * 
 * @author scott-@-adligo
 *
 */
public class BigQueryLog4jConfig {

  public BigQueryLog4jConfig() {
    Logger.getLogger(BigQueryToPubSub.class).setLevel(Level.DEBUG);
    Logger.getLogger(DelegatingPTransform.class).setLevel(Level.DEBUG);
    Logger.getLogger(Done.class).setLevel(Level.DEBUG);
    Logger.getLogger(TableRowToStringTransformer.class).setLevel(Level.DEBUG);
  }
}
