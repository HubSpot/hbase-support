package com.hubspot.hbase.tasks.balancing.annealing;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.balancing.Balancer;
import com.hubspot.hbase.tasks.balancing.annealing.perturbers.Perturber;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.hbase.tasks.balancing.cost.CostFunction;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AnnealingBalancer implements Balancer {
  private AssignmentConfig initial;

  private final CostFunction scorer;
  private final TemperatureFunction temperatureFunction;
  private final AcceptanceFunction acceptanceFunction;
  private final Perturber perturber;
  private final int stagnateIterations;

  @Inject
  public AnnealingBalancer(@Named(OptimizationModule.COST_FUNCTION) final CostFunction scorer,
                           @Named(OptimizationModule.TEMP_FUNCTION) final TemperatureFunction temperatureFunction,
                           @ForArg(HBaseTaskOption.BALANCE_STAGNATE_ITER) final Optional<Integer> stagnateIterations,
                           final AcceptanceFunction acceptanceFunction,
                           final Perturber perturber) {
    this.scorer = scorer;
    this.temperatureFunction = temperatureFunction;
    this.acceptanceFunction = acceptanceFunction;
    this.perturber = perturber;
    this.stagnateIterations = stagnateIterations.get();
  }

  @Override
  public void setRegionInformation(final Multimap<ServerName, RegionStats> regionInfo, final ImmutableSet<ServerName> servers) {
    initial = new AssignmentConfig(regionInfo, servers);
  }

  @Override
  public Map<HRegionInfo, ServerName> computeTransitions(final int timeLimitSeconds, final int maxTransitions) throws Exception {
    int targetSteps = 100 * timeLimitSeconds;
    final Stopwatch stopwatch = new Stopwatch().start();
    AssignmentConfig current = initial;
    scorer.setInitialData(Lists.newArrayList(initial.getAssignmentsByServer().values()), initial.getServers());
    double currentScore = scorer.computeCost(current.getAssignments());
    AssignmentConfig best = initial;
    double bestScore = currentScore;

    int i, lastImprovement = 0;

    for (i = 0; i < Integer.MAX_VALUE; ++i) {
      if (stopwatch.elapsedTime(TimeUnit.SECONDS) > timeLimitSeconds) break;

      final double temp = i >= targetSteps ? 0 : temperatureFunction.temperature(i, targetSteps);
      AssignmentConfig next = perturber.perturb(current, temp);
      double nextScore = scorer.computeCost(next.getAssignments());
      if (acceptanceFunction.acceptNewState(currentScore, nextScore, temp)) {
        current = next;
        currentScore = nextScore;
      }
      if (nextScore < bestScore) {
        best = next;
        bestScore = nextScore;
        lastImprovement = i;
      }

      if (lastImprovement - i > stagnateIterations) {
        System.out.println("Stopping optimization due to stagnation.");
        break;
      }
    }
    stopwatch.stop();

    System.out.println(String.format("Ran %s steps", i));

    System.out.println(scorer.prettyPrint("Initial", initial.getAssignments()));

    System.out.println(scorer.prettyPrint("Final", best.getAssignments()));

    return best.getResultMap(initial);
  }

}
