package com.hubspot.hbase.tasks.balancing.annealing.perturbers;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hubspot.hbase.tasks.balancing.annealing.AssignmentConfig;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.hbase.tasks.balancing.cost.CostFunction;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static com.hubspot.hbase.tasks.balancing.annealing.Iterables3.pickRandom;

public class SomewhatGreedyPerturber implements Perturber {
  private final Random random = new Random();

  private final CostFunction costFunction;

  @Inject
  public SomewhatGreedyPerturber(@Named(OptimizationModule.COST_FUNCTION) final CostFunction costFunction) {
    this.costFunction = costFunction;
  }

  @Override
  public AssignmentConfig perturb(final AssignmentConfig initial, final double temp) {
    final Map<ServerName, Double> serverCosts = getServerCosts(initial.getAssignmentsByServer());
    final List<Map.Entry<ServerName, Double>> orderedCosts = Lists.newArrayList(serverCosts.entrySet());

    Collections.sort(orderedCosts, new Comparator<Entry<ServerName, Double>>() {
      @Override
      public int compare(final Entry<ServerName, Double> o1, final Entry<ServerName, Double> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    int heaviestServerIndex = orderedCosts.size() - 1;
    heaviestServerIndex -= random.nextInt(Math.max(1, Math.min(orderedCosts.size() - 1, (int)(temp * orderedCosts.size() * .4))));
    final ServerName lightestServer = orderedCosts.get(0).getKey();
    final ServerName heaviestServer = orderedCosts.get(heaviestServerIndex).getKey();

    for (int i = 0; i < 10000; ++i) {
      final Collection<RegionStats> regions = initial.getAssignmentsByServer().get(heaviestServer);
      final Set<RegionStats> subList = Sets.newHashSet();

      final int targetSize = Math.min(Math.max((int)((1 - temp) * regions.size()), 1), regions.size());

      for (int j = 0; j < 1000; ++j) {
        subList.add(pickRandom(regions));
        if (subList.size() >= targetSize) break;
      }

      double greatestImprovement = Double.MIN_VALUE;
      RegionStats regionToMove = null;

      for (final RegionStats region : subList) {
        final double improvement = serverCosts.get(initial.getAssignment(region)) - serverCosts.get(lightestServer);
        if (improvement > greatestImprovement) {
          greatestImprovement = improvement;
          regionToMove = region;
        }
      }

      if (regionToMove == null) {
        continue;
      }

      final Optional<AssignmentConfig> result = initial.assign(regionToMove, lightestServer);
      if (result.isPresent()) {
        return result.get();
      }
    }
    return initial;
  }

  private Map<ServerName, Double> getServerCosts(Multimap<ServerName, RegionStats> regions) {
    Map<ServerName, Double> result = Maps.newHashMap();

    for (final ServerName server : regions.keySet()) {
      result.put(server, costFunction.getServerCost(server, regions.get(server)));
    }
    return result;
  }
}
