package com.hubspot.hbase.tasks.balancing.annealing.perturbers;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.hubspot.hbase.tasks.balancing.annealing.AssignmentConfig;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.hbase.tasks.balancing.cost.CostFunction;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hubspot.hbase.tasks.balancing.annealing.Iterables3.pickRandom;

public class GreedyPerturber implements Perturber {
  private final CostFunction costFunction;

  @Inject
  public GreedyPerturber(@Named(OptimizationModule.COST_FUNCTION) final CostFunction costFunction) {
    this.costFunction = costFunction;
  }

  @Override
  public AssignmentConfig perturb(final AssignmentConfig initial, final double temp) {
    final Map<ServerName, Double> serverCosts = getServerCosts(initial.getAssignmentsByServer(), initial.getServers());
    final List<Map.Entry<ServerName, Double>> orderedCosts = Lists.newArrayList(serverCosts.entrySet());

    Collections.sort(orderedCosts, new Comparator<Map.Entry<ServerName, Double>>() {
      @Override
      public int compare(final Map.Entry<ServerName, Double> serverNameDoubleEntry, final Map.Entry<ServerName, Double> serverNameDoubleEntry2) {
        return Doubles.compare(serverNameDoubleEntry2.getValue(), serverNameDoubleEntry.getValue());
      }
    });

    final Multimap<ServerName, RegionStats> assignments = initial.getAssignmentsByServer();

    while (true) {
      Optional<AssignmentConfig> maybeConfig = initial.assign(pickRandom(assignments.get(orderedCosts.get(0).getKey())), orderedCosts.get(orderedCosts.size() - 1).getKey());
      if (maybeConfig.isPresent()) {
        return maybeConfig.get();
      }
    }
  }

  private Map<ServerName, Double> getServerCosts(Multimap<ServerName, RegionStats> regions, Set<ServerName> allServers) {
    Map<ServerName, Double> result = Maps.newHashMap();

    for (final ServerName server : regions.keySet()) {
      result.put(server, costFunction.getServerCost(server, regions.get(server)));
    }

    for (final ServerName server : allServers) {
      if (!result.containsKey(server)) {
        result.put(server, 0d);
      }
    }

    return result;
  }
}
