package com.google.cloud.teleport.bigquery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

public class TableRowToStringTransformer extends PTransform<PCollection<TableRow>, PCollection<String>> {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final Logger log;
  
  public TableRowToStringTransformer() {
    this(LoggerFactory.getLogger(TableRowToStringTransformer.class));
  }
  
  public TableRowToStringTransformer(Logger log) {
    this.log = log;
  }
  
  @Override
  public PCollection<String> expand(PCollection<TableRow> input) {
    // TODO Auto-generated method stub
    return null;
  }

  
  public String convertJsonToTableRow(TableRow tr) {
    TableRow row;
    if (tr != null) {
      List<TableCell> cells = tr.getF();
      if (log.isDebugEnabled()) {
        log.debug(String.format("converted the following row to; '%s'\n\t") + tr);
      }
      return "" + cells.get(0);
    }
    return null;
  }
}
