package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TransitionCost extends CostFunction {
  private final Map<RegionStats, ServerName> index = Maps.newHashMap();
  private final int totalTransitions;

  public TransitionCost(final double weight, final int totalTransitions) {
    super(weight);
    this.totalTransitions = totalTransitions;
  }

  @Override
  public void setInitialData(final List<RegionStats> regions, ImmutableSet<ServerName> servers) {
    for (final RegionStats region : regions) {
      index.put(region, region.getServerName());
    }
  }

  @Override
  public double getServerCost(ServerName serverName, final Collection<RegionStats> regions) {
    return 0;
  }

  @Override
  public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    double totalAssignments = 0;
    for (final RegionAssignment assignment : assignments) {
      if (!Objects.equal(assignment.getNewServer(), index.get(assignment.getRegionStats()))) {
        totalAssignments++;
      }
    }
    double scale = (totalAssignments < this.totalTransitions) ? .001 : 1;
    return scale * Math.sqrt(totalAssignments / index.size());
  }
}
