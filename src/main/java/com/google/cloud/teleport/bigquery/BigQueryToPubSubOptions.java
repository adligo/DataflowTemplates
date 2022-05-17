package com.google.cloud.teleport.bigquery;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.Objects;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.http.util.CharArrayBuffer;
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
  public static final String THERE_WAS_A_PROBLEM_READING_THE_FOLLOWING_FILE = "There was a problem reading the following file;\n\t";
  public static final String BIG_QUERY_TO_PUB_SUB_OPTIONS_REQUIRES_A_RUN_OPTIONS = "BigQueryToPubSubOptions requires a Run Options!";
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
  private BigQueryToPubSubRunOptions runOpts;
  private String inputQuery;
  private String inputQueryFilePath;
  
  public BigQueryToPubSubOptions() {
    this(LoggerFactory.getLogger(BigQueryToPubSubOptions.class));
  }
  
  public BigQueryToPubSubOptions(Logger log) {
    this.log = Objects.requireNonNull(log);
  }
  public String getInputQuery() {
    return inputQuery;
  }

  public String getInputQueryFilePath() {
    return inputQueryFilePath;
  }

  public BigQueryToPubSubRunOptions getRunOptions() {
    return runOpts;
  }

  public Logger getLog() {
    return log;
  }

  public BigQueryToPubSubOptions setRunOptions(BigQueryToPubSubRunOptions runOpts) {
    this.runOpts = runOpts;
    return this;
  }
  
  public BigQueryToPubSubOptions setInputQuery(String inputQuery) {
    this.inputQuery = inputQuery;
    return this;
  }

  public BigQueryToPubSubOptions setInputQueryFilePath(String inputQueryFilePath) {
    this.inputQueryFilePath = inputQueryFilePath;
    return this;
  }
  
  public BigQueryToPubSubRunOptions updateInputQueryFromFilePath() {
    if (inputQuery == null || inputQuery.trim().length() == 0) {
      if (inputQueryFilePath == null || inputQueryFilePath.trim().length() == 0) {
        throw new IllegalStateException(EITHER_A_INPUT_QUERY_OR_INPUT_QUERY_FILE_PATH_WITH_A_VAILD_QUERY_IS_REQUIRED);
      } else {
        char [] buffer = new char[64];
        StringBuilder sb = new StringBuilder();
        try (FileReader fr = new FileReader(inputQueryFilePath)) {
          fr.read(buffer);
          sb.append(buffer);
        } catch (IOException x) {
          throw new RuntimeException(THERE_WAS_A_PROBLEM_READING_THE_FOLLOWING_FILE + inputQueryFilePath, x);
        }
        inputQuery = sb.toString();
        runOpts.setInputQuery(inputQuery);
      }
    } else {
      runOpts.setInputQuery(inputQuery);
      //runOpts.setInputQuery(new SimpleValueProvider<String>(inputQuery));
    } 
    if (log.isInfoEnabled()) {
      log.info("inputQuery is; " + inputQuery);
    }
    return runOpts;
  }

}
