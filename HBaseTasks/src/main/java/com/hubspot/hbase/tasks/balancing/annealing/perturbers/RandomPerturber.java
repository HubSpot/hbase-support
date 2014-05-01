package com.hubspot.hbase.tasks.balancing.annealing.perturbers;

import com.google.common.base.Objects;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.balancing.annealing.AssignmentConfig;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;

import static com.hubspot.hbase.tasks.balancing.annealing.Iterables3.pickRandom;

public class RandomPerturber implements Perturber {
  private final int maxTransitions;

  @Inject
  public RandomPerturber(@Named(OptimizationModule.TOTAL_TRANSITIONS) final int maxTransitions) {
    this.maxTransitions = maxTransitions;
  }

  @Override
  public AssignmentConfig perturb(final AssignmentConfig initial, final double temp) {
    final Iterable<RegionAssignment> assignments = initial.getAssignments();
    final Collection<ServerName> servers = initial.getServers();

    AssignmentConfig result = initial;

    for (int i = 0; i < Math.max(1, temp * maxTransitions * .5); ++i) {
      final RegionAssignment oldAssignment = pickRandom(assignments);
      ServerName newServer = pickRandom(servers);
      while (servers.size() > 1 && !Objects.equal(newServer, oldAssignment.getNewServer())) {
        newServer = pickRandom(servers);
      }
      result = result.assign(oldAssignment.getRegionStats(), newServer).or(result);
    }
    return result;
  }
}
