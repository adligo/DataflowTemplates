package com.google.cloud.teleport.bigquery;

import java.util.Objects;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.transforms.PTransform;

import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.util.DelegatingPTransform;

/**
 * com.google.cloud.teleport.templates.DatastoreToBigQuery
 * 
 * @author scott-@-adligo
 *
 * @param <InputT>
 * @param <OutputT>
 */
public class BigQueryProducer
extends PTransform<PBegin, PCollection<TableRow>> {

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
    this.log = Objects.requireNonNull(log, DelegatingPTransform.A_LOG_IS_REQUIRED);
    this.options = Objects.requireNonNull(options, DelegatingPTransform.OPTIONS_ARE_REQUIRED);
  }

  @Override
  public PCollection<TableRow> expand(PBegin input) {
    if (log.isInfoEnabled()) {
      log.info(EXPANDING_WITH_QUERY + options.getInputQuery());
    }
    
    //BigtableSource source =
     //   new BigtableSource(getBigtableConfig(), getBigtableReadOptions(), null);
    //return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(
    TypedRead<TableRow> tr = BigQueryIO.readTableRows()
        .fromQuery(options.getInputQuery())
        .withoutValidation()
        .withTemplateCompatibility()
        .usingStandardSql();
    if (log.isInfoEnabled()) {
      log.info("TypedRead is " + tr);
    }
    return tr.expand(input);
   
    //tr.
    //return input.getPipeline().apply(tr);
  }

}
