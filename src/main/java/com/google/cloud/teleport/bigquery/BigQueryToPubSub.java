/*
 * Copyright (C) 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigquery;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.util.SimpleValueProvider;

/**
 * A Dataflow pipeline to stream <a href="https://avro.apache.org/">Apache Avro</a> records from
 * Pub/Sub into a BigQuery table.
 *
 * <p>Any persistent failures while writing to BigQuery will be written to a Pub/Sub dead-letter
 * topic.
 */
public final class BigQueryToPubSub {

  
  /**
   * Validates input flags and executes the Dataflow pipeline.
   *
   * @param args command line arguments to the pipeline
   */
  public static void main(String[] args) {
    new BigQueryLog4jConfig();
    new BigQueryToPubSub(new BigQueryToPubSubOptions()
        .setLog(LoggerFactory.getLogger(BigQueryToPubSub.class))
        .setOptions(PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigQueryToPubSubRunOptions.class)
            )).run();
  }


  private BigQueryToPubSubRunOptions options;
  private Logger log;
  
  
  public BigQueryToPubSub(BigQueryToPubSubOptions opts) {
    options = Objects.requireNonNull(opts.getOptions());
    log = Objects.requireNonNull(opts.getLog());
  }
  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options execution parameters to the pipeline
   * @return result of the pipeline execution as a {@link PipelineResult}
   */
  @SuppressWarnings("unchecked")
  private void run() {
    if (log.isInfoEnabled()) {
      log.info("Starting run!");
    }

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("Read from BigQuery",new BigQueryProducer(options));
    pipeline.run(); 
    if (log.isInfoEnabled()) {
      log.info("Run complete!");
    }
  }
}


