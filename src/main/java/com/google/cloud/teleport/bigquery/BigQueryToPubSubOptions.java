package com.google.cloud.teleport.bigquery;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Objects;

import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.teleport.util.SimpleValueProvider;

/**
 * This provides the values to the BigQueryToPubSub instance.
 * 
 * @author scott-@-adligo
 *
 */
public class BigQueryToPubSubOptions {
  public static final String EMPTY_STRING = "";
  public static final String READING_THE_FOLLOWING_BYTE_COUNT = "Reading the following byte count; ";
  public static final String EITHER_A_INPUT_QUERY_OR_INPUT_QUERY_FILE_PATH_WITH_A_VAILD_QUERY_IS_REQUIRED = "Either a inputQuery or inputQueryFilePath with a vaild query is required!";
  public static final String NO_INPUT_QUERY_FILE_PATH_PROVIDED = "No inputQueryFilePath provided.";
  public static final String READ_QUERY = "Read query;\n";
  public static final String THE_FOLLOWING_FILE_IS_TO_BIG = "The following file is to big; ";
  public static final String UNABLE_TO_LOAD = "Unable to load; ";
  public static final String LOADING_QUERY_FROM = "Loading query from ; ";

  /**
   * Load the query as a string from the filePath.
   * @param filePath
   * @return
   */
  public static String loadQueryFromFile(String filePath) {
    return loadQueryFromFile(filePath, LoggerFactory.getLogger(BigQueryToPubSubOptions.class));
  }

  /**
   * Load the query as a string from the filePath.
   * @param filePath
   * @param log pass this in for simplified testing of 
   *   info, debug and exception assertions.  Microsoft C# style.
   * @return
   */
  public static String loadQueryFromFile(String filePath, Logger log) {
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
      if (log.isInfoEnabled()) {
        log.info(READING_THE_FOLLOWING_BYTE_COUNT + s);
      }
      CharBuffer cb = CharBuffer.allocate(s);
      fr.read(cb);
      String query = new String(cb.array());
      if (log.isInfoEnabled()) {
        log.info(READ_QUERY + query);
      }
      return query;
    } catch (IOException x) {
      throw new RuntimeException(UNABLE_TO_LOAD + filePath, x);
    }
  }
  
  
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
      String query = loadQueryFromFile(filePath, log);
      opts.setInputQuery(new SimpleValueProvider<String>(query));
    } else {
      if (log.isInfoEnabled()) {
        log.info(NO_INPUT_QUERY_FILE_PATH_PROVIDED);
      }
    }
    Object inputQuery = opts.getInputQuery();
    if (log.isInfoEnabled()) {
      log.info("inputQuery is; " + inputQuery);
    }
    if (inputQuery == null || EMPTY_STRING.equals(inputQuery)) {
      throw new IllegalStateException(EITHER_A_INPUT_QUERY_OR_INPUT_QUERY_FILE_PATH_WITH_A_VAILD_QUERY_IS_REQUIRED);
    }
    return opts;
  }
}
