package com.hubspot.hbase.coprocessor;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

import java.io.IOException;

public class SetCounter<T> {
  public static final float ERROR = 0.005f;
  private HyperLogLog hll;

  public SetCounter() {
    this.hll = new HyperLogLog(ERROR);
  }

  public SetCounter(float error) {
    this.hll = new HyperLogLog(error);
  }

  private SetCounter(final HyperLogLog hyperLogLog) {
    this.hll = hyperLogLog;
  }

  public byte[] toBytes() {
    try {
      return Gzip.compress(hll.getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void mergeWith(final SetCounter<T> setCounter) {
    try {
      this.hll.addAll(setCounter.hll);
    } catch (CardinalityMergeException e) {
      throw new RuntimeException(e);
    }
  }

  public void offer(final T object) {
    this.hll.offer(object);
  }

  public long cardinality() {
    return this.hll.cardinality();
  }

  public static <T> SetCounter<T> getCounter(final byte[] bytes) {
    try {
      final HyperLogLog hyperLogLog = HyperLogLog.Builder.build(Gzip.decompress(bytes));
      return new SetCounter<T>(hyperLogLog);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
