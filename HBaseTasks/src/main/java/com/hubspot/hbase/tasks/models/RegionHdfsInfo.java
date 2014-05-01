package com.hubspot.hbase.tasks.models;

import org.apache.hadoop.hbase.ServerName;

import java.io.Serializable;
import java.util.List;

public class RegionHdfsInfo implements Serializable {
  private List<HdfsBlockInfo> hdfsBlockInfos;

  public RegionHdfsInfo() {
  }

  public RegionHdfsInfo(final List<HdfsBlockInfo> hdfsBlockInfos) {
    this.hdfsBlockInfos = hdfsBlockInfos;
  }

  public long transferCost(final ServerName serverName) {
    long total = 0L;
    for (final HdfsBlockInfo blockInfo : hdfsBlockInfos) {
      total += blockInfo.transferCost(serverName);
    }
    return total;
  }

  public List<HdfsBlockInfo> getHdfsBlockInfos() {
    return hdfsBlockInfos;
  }

  public void setHdfsBlockInfos(final List<HdfsBlockInfo> hdfsBlockInfos) {
    this.hdfsBlockInfos = hdfsBlockInfos;
  }
}
