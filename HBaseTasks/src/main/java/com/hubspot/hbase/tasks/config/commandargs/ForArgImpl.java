package com.hubspot.hbase.tasks.config.commandargs;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.lang.annotation.Annotation;

public class ForArgImpl implements ForArg, Serializable {
  private final HBaseTaskOption value;

  public ForArgImpl(HBaseTaskOption value) {
    this.value = Preconditions.checkNotNull(value);
  }

  @Override
  public Class<? extends Annotation> annotationType() {
    return ForArg.class;
  }

  @Override
  public int hashCode() {
    return (127 * "value".hashCode()) ^ value.hashCode();
  }

  @Override
  public String toString() {
    return "@ForArg(value=" + value + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ForArgImpl forArg = (ForArgImpl) o;

    return value == forArg.value;
  }

  @Override
  public HBaseTaskOption value() {
    return value;
  }

  private static final long serialVersionUID = 0;
}
