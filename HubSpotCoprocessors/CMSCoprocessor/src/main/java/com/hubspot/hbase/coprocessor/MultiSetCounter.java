package com.hubspot.hbase.coprocessor;

import java.util.List;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;

public class MultiSetCounter {

  private CountMinSketch cms;

  public MultiSetCounter(int depth, int width, int seed) {
    cms = new CountMinSketch(depth, width, seed);
  }

  public MultiSetCounter(double epsOfTotalCount, double confidence, int seed) {
    cms = new CountMinSketch(epsOfTotalCount, confidence, seed);
  }

  public MultiSetCounter(CountMinSketch cms) {
    this.cms = cms;
  }

  public byte[] toBytes() {
    return Gzip.compress(CountMinSketch.serialize(cms));
  }

  public void mergeWith(MultiSetCounter multiSetCounter) {
    try {
      this.cms = CountMinSketch.merge(cms, multiSetCounter.cms);
    } catch (FrequencyMergeException e) {
      throw new RuntimeException(e);
    }
  }

  public void offer(final long item) {
    offer(item, 1);
  }

  public void offer(final long item, long count) {
    cms.add(item, count);
  }

  public void offer(final String item) {
    offer(item, 1);
  }

  public void offer(final String item, long count) {
    cms.add(item, count);
  }

  public long cardinality(final long item) {
    return cms.estimateCount(item);
  }

  public long cardinality(final String item) {
    return cms.estimateCount(item);
  }

  public SummaryStatistics getSummary(List<Long> items) {
    SummaryStatistics stats = new SummaryStatistics();
    for (long item : items) {
      for (int i = 0; i < cardinality(item); i++) {
        stats.addValue(item);
      }
    }
    return stats;
  }

  public static MultiSetCounter getCounter(final byte[] bytes) {
    return new MultiSetCounter(CountMinSketch.deserialize(Gzip.decompress(bytes)));
  }

}
