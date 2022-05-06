package com.google.cloud.teleport.bigquery;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Objects;

import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;

import com.google.cloud.teleport.util.SimpleValueProvider;

/**
 * This provides the values to the BigQueryToPubSub instance.
 * 
 * @author scott-@-adligo
 *
 */
public class BigQueryToPubSubOptions {
  public static final String EITHER_A_INPUT_QUERY_OR_INPUT_QUERY_FILE_PATH_WITH_A_VAILD_QUERY_IS_REQUIRED = "Either a inputQuery or inputQueryFilePath with a vaild query is required!";
  public static final String NO_INPUT_QUERY_FILE_PATH_PROVIDED = "No inputQueryFilePath provided.";
  public static final String READ_QUERY = "Read query;\n";
  public static final String THE_FOLLOWING_FILE_IS_TO_BIG = "The following file is to big; ";
  public static final String UNABLE_TO_LOAD = "Unable to load; ";
  public static final String LOADING_QUERY_FROM = "Loading query from ; ";

  private Logger log;
  private BigQueryToPubSubRunOptions options;

  public BigQueryToPubSubRunOptions getOptions() {
    return options;
  }

  public Logger getLog() {
    return log;
  }

  public BigQueryToPubSubOptions setOptions(BigQueryToPubSubRunOptions options) {
    this.options = updateInputQueryFromFilePath(Objects.requireNonNull(options));
    return this;
  }

  public BigQueryToPubSubOptions setLog(Logger log) {
    this.log = Objects.requireNonNull(log);
    return this;
  }

  public BigQueryToPubSubRunOptions updateInputQueryFromFilePath(BigQueryToPubSubRunOptions opts) {
    ValueProvider<String> vp = opts.getInputQueryFilePath();
    if (vp.isAccessible()) {
      String filePath = vp.get();
      if (log.isInfoEnabled()) {
        log.info(LOADING_QUERY_FROM + filePath);
      }
      File f = new File(filePath);
      try (FileReader fr = new FileReader(filePath)) {
        long size = f.length();
        long max = Integer.MAX_VALUE;
        if (size > max) {
          throw new RuntimeException(THE_FOLLOWING_FILE_IS_TO_BIG + size + " " + filePath);
        }
        int s = (int) size;
        CharBuffer cb = CharBuffer.allocate(s);
        fr.read(cb);
        String query = cb.toString();
        if (log.isInfoEnabled()) {
          log.info(READ_QUERY + query);
        }
        opts.setInputQuery(new SimpleValueProvider<String>(query));
      } catch (IOException x) {
        throw new RuntimeException(UNABLE_TO_LOAD + filePath, x);
      }
    } else {
      if (log.isInfoEnabled()) {
        log.info(NO_INPUT_QUERY_FILE_PATH_PROVIDED);
      }
    }
    if (!opts.getInputQuery().isAccessible()) {
      throw new IllegalStateException(EITHER_A_INPUT_QUERY_OR_INPUT_QUERY_FILE_PATH_WITH_A_VAILD_QUERY_IS_REQUIRED);
    }
    return opts;
  }
}
