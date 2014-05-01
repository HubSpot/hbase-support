package com.hubspot.hbase.tasks.balancing;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.Map;

public interface Balancer {
  public void setRegionInformation(Multimap<ServerName, RegionStats> regionInfo, ImmutableSet<ServerName> servers);
  public Map<HRegionInfo, ServerName> computeTransitions(int timeLimitSeconds, int maxTransitions) throws Exception;
}
