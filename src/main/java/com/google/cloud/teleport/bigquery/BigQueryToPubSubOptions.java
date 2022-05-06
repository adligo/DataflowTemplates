package com.google.cloud.teleport.bigquery;

import org.slf4j.Logger;

public class BigQueryToPubSubOptions {
  private Logger log;
  private BigQueryToPubSubRunOptions options;

  
  public BigQueryToPubSubRunOptions getOptions() {
    return options;
  }
  public Logger getLog() {
    return log;
  }
  public BigQueryToPubSubOptions setOptions(BigQueryToPubSubRunOptions options) {
    this.options = options;
    return this;
  }
  public BigQueryToPubSubOptions setLog(Logger log) {
    this.log = log;
    return this;
  }
  
}
