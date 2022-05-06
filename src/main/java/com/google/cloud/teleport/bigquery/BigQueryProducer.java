package com.google.cloud.teleport.bigquery;

import java.util.Objects;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;

import org.apache.beam.sdk.transforms.PTransform;

import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.util.DelegatingPTransform;

public class BigQueryProducer<InputT extends PBegin, OutputT extends PCollection<TableRow>>
extends DelegatingPTransform<PInput, POutput> {

  public static final String EXPANDING_WITH_QUERY = "Expanding with query;\n\t";
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final Logger log;
  private final BigQueryToPubSubRunOptions options;

  public BigQueryProducer(BigQueryToPubSubRunOptions options) {
    this(options, LoggerFactory.getLogger(BigQueryProducer.class));
  }
  
  public BigQueryProducer(BigQueryToPubSubRunOptions options, Logger log) {
    super(BigQueryIO.readTableRows()
        .fromQuery(options.getInputQuery())
        .withoutValidation()
        .withTemplateCompatibility()
        .usingStandardSql());
    this.log = Objects.requireNonNull(log, A_LOG_IS_REQUIRED);
    this.options = Objects.requireNonNull(options, OPTIONS_ARE_REQUIRED);
  }

  @Override
  public POutput expand(PInput input) {
    if (log.isInfoEnabled()) {
      log.info(EXPANDING_WITH_QUERY + options.getInputQuery());
    }
    return super.expand(input);
  }

}
