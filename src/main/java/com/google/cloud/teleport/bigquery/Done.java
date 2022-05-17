package com.google.cloud.teleport.bigquery;

import java.util.Collection;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Done extends PTransform<PCollection, PDone>  {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private final PDone done;
  private final Logger log;
  
  public Done(Pipeline pipeline) {
    this(pipeline, LoggerFactory.getLogger(Done.class));
  }
  
  public Done(Pipeline pipeline, Logger log) {
    done = PDone.in(pipeline);
    this.log = Objects.requireNonNull(log);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
  @Override
  public PDone expand(PCollection input) {
    PCollectionView<Iterable<?>> output = (PCollectionView)
        input.apply(View.asIterable());
    if (log.isDebugEnabled()) {
      Collection<PValue> vals = output.expand().values();
      for (PValue v: vals) {
        log.debug("Value is " + v);
        log.debug("" + v.getClass().getName());
        PCollection vc = (PCollection) v;
        PCollectionView<Iterable<?>> ovc = (PCollectionView)
            vc.apply(View.asIterable());
        Collection<PValue> jovs = ovc.expand().values();
        for (PValue j: jovs) {
          log.debug("Value is " + j);
          log.debug("" + j.getClass().getName());
        }
      }
    }
    return done;
  }

}
