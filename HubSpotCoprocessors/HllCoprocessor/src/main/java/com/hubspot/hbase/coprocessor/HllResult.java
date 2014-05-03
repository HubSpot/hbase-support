package com.hubspot.hbase.coprocessor;

public class HllResult {
  private final long value;
  private final long timestamp;

  public HllResult(final long value, final long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }

  public long getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final HllResult hllResult = (HllResult) o;

    if (timestamp != hllResult.timestamp) return false;
    if (value != hllResult.value) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (value ^ (value >>> 32));
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return String.format("HllResult{value=%s, timestamp=%s}", value, timestamp);
  }
}
