package com.hubspot.hbase.coprocessor;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class HashedBytes implements Writable, Serializable {
  private static final long serialVersionUID = 1L;
  private byte[] bytes;

  public HashedBytes(final byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    WritableUtils.writeCompressedByteArray(out, bytes);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    this.bytes = WritableUtils.readCompressedByteArray(in);
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final HashedBytes that = (HashedBytes) o;
    return Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return bytes != null ? Arrays.hashCode(bytes) : 0;
  }
}
