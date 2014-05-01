package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.Set;

public class MigrationCost extends CostFunction {

  private Set<String> targetServers;

  protected MigrationCost(double weight) {
    super(weight);

    targetServers = null;
  }

  @Inject
  public void setTargetServers(@ForArg(HBaseTaskOption.SERVER_NAME) Optional<String> servers) {
    targetServers = HBaseTaskOption.getCommaSeparatedAsList(servers);
    System.out.println("Migration cost initialized, migrating to " + servers.get());
  }

  @Override
  protected double actuallyComputeCost(Iterable<RegionAssignment> assignments) {
    int total = 0;
    for (RegionAssignment assignment : assignments) {
      if (targetServers != null && !targetServers.contains(assignment.getNewServer().getHostname())) {
        total++;
      }
    }

    return total;
  }

  @Override
  public double getServerCost(ServerName serverName, Collection<RegionStats> regions) {
    return targetServers != null && serverName != null && targetServers.contains(serverName.getHostname()) ? 0 : regions.size();
  }
}
