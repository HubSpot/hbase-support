package com.hubspot.hbase.tasks.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.ServerName;

import java.util.Set;

public class HdfsBlockInfo {
  private final long size;
  private final Set<ServerName> replicas;

  @JsonCreator
  public HdfsBlockInfo(@JsonProperty("size") final long size, @JsonProperty("replicas") final Set<ServerName> replicas) {
    this.size = size;
    this.replicas = replicas;
  }

  public long getSize() {
    return size;
  }

  public Set<ServerName> getReplicas() {
    return replicas;
  }

  public long transferCost(final ServerName exampleServer) {
    return replicas.contains(exampleServer) ? 0L : size;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
            .add("size", size)
            .add("replicas", replicas)
            .toString();
  }
}
