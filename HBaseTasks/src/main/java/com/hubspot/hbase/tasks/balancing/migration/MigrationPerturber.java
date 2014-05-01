package com.hubspot.hbase.tasks.balancing.migration;

import com.google.common.base.Optional;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.balancing.annealing.perturbers.GreedyPerturber;
import com.hubspot.hbase.tasks.balancing.annealing.perturbers.TableGreedyPerturber;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.hbase.tasks.balancing.cost.CostFunction;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Map;
import java.util.Set;

public class MigrationPerturber extends TableGreedyPerturber {

  private final Set<String> targetServers;
  private final Set<String> sourceServers;

  @Inject
  public MigrationPerturber(@Named(OptimizationModule.COST_FUNCTION) CostFunction costFunction,
                            GreedyPerturber greedyPerturber,
                            @ForArg(HBaseTaskOption.SERVER_NAME) Optional<String> serverName,
                            @ForArg(HBaseTaskOption.SOURCE_SERVER_NAME) Optional<String> sourceServerName) {
    super(costFunction, greedyPerturber);

    HBaseTaskOption.validateConflictingArgs(String.format("MigrationPerturber can migrate FROM a set of servers, or TO a set of servers.  Choose either -%s or -%s, but not both.", HBaseTaskOption.SERVER_NAME, HBaseTaskOption.SOURCE_SERVER_NAME),
            serverName, sourceServerName);

    targetServers = HBaseTaskOption.getCommaSeparatedAsList(serverName);
    sourceServers = HBaseTaskOption.getCommaSeparatedAsList(sourceServerName);
  }

  @Override
  protected Map<ServerName, Double> getServerCosts(Multimap<ServerName, RegionStats> regions, String tableName, Set<ServerName> servers) {
    Map<ServerName, Double> result = super.getServerCosts(regions, tableName, servers);

    double maxCost = Double.MIN_VALUE;
    for (Double cost : result.values()) {
      if (cost > maxCost) {
        maxCost = cost;
      }
    }

    for (ServerName server : result.keySet()) {
      if (shouldElevate(server)) {
        result.put(server, result.get(server) + maxCost + 1);
      }
    }

    return result;
  }

  private boolean shouldElevate(ServerName server) {
    return (!sourceServers.isEmpty() && sourceServers.contains(server.getHostname())) || (!targetServers.isEmpty() && !targetServers.contains(server.getHostname()));
  }
}
