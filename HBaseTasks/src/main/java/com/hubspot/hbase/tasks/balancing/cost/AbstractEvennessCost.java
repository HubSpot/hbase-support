package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.List;

public class AbstractEvennessCost extends CostFunction {
  private final Function<RegionStats, ? extends Number> statFunction;
  private final Function<RegionAssignment, ? extends Number> assignmentFunction;
  private double averageStat = 0.0;
  private ImmutableSet<ServerName> servers;

  public AbstractEvennessCost(final double weight, final Function<RegionStats, ? extends Number> statFunction) {
    super(weight);
    this.statFunction = statFunction;
    this.assignmentFunction = Functions.compose(statFunction, new Function<RegionAssignment, RegionStats>(){
      @Override
      public RegionStats apply(final RegionAssignment input) {
        return input.getRegionStats();
      }
    });
  }

  protected ImmutableSet<ServerName> getServers() {
    return servers;
  }

  @Override
  public void setInitialData(final List<RegionStats> regions, ImmutableSet<ServerName> servers) {
    double totalStat = 0;
    for (final RegionStats region : regions) {
      totalStat += statFunction.apply(region).doubleValue();
    }
    averageStat = totalStat / servers.size();
    this.servers = servers;
  }

  @Override
  public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    return computeUnevenness(assignments, assignmentFunction, servers);
  }

  @Override
  public double getServerCost(ServerName serverName, final Collection<RegionStats> regions) {
    double total = 0;
    for (final RegionStats region : regions) {
      total += statFunction.apply(region).doubleValue();
    }
    return total / averageStat / getServerWeight(serverName);
  }
}
