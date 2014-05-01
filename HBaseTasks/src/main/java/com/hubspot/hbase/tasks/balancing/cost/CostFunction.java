package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class CostFunction {
  private static final Log LOG = LogFactory.getLog(CostFunction.class);

  private final double weight;

  protected CostFunction(final double weight) {
    this.weight = weight;
  }

  public void setInitialData(final List<RegionStats> regions, ImmutableSet<ServerName> servers) {
  }

  protected abstract double actuallyComputeCost(final Iterable<RegionAssignment> assignments);

  public abstract double getServerCost(ServerName serverName, final Collection<RegionStats> regions);

  public final double computeCost(final Iterable<RegionAssignment> assignments) {
    final Stopwatch stopwatch = new Stopwatch().start();
    final double result = actuallyComputeCost(assignments);
    LOG.trace(String.format("%s - completed in %sms", getClass().getName(), stopwatch.stop().elapsedMillis()));
    return result;
  }

  public double getWeight() {
    return weight;
  }

  /**
   * Compute a normalized uneveness function. Given a set a numbers, we try to define a function such that:
   * <p/>
   * Complete evenness = 0
   * Complete unnevenness = 1
   * <p/>
   * Complete unnevenness can be defined as something like 100,0,0,0
   * <p/>
   * The definition I settled on was: 1 - e^(-N*σ/µ)
   * <p/>
   * Let's walk through some examples:
   * <p/>
   * Perfect balancing:
   * <p/>
   * If we are perfectly balanced, σ = 0, then 1 - e^0 = 0.
   * <p/>
   * Perfectly inbalanced:
   * <p/>
   * Look at 100,0,0,0
   * <p/>
   * µ = 25
   * N*σ = sqrt(75^2) = 75
   * 1 - e^(-75 / 25) = 0.95
   * <p/>
   * It's not 1, but it's pretty close.
   * <p/>
   * Any time we have a cost function that needs to determine cost based on unevenness, we can pas it here and
   * get a normalized cost between 0 and 1.
   */
  protected double unevenness(Collection<? extends Number> numbers) {
    if (numbers.isEmpty()) return 0;
    double sum = 0.0;
    double squared_sum = 0.0;
    for (final Number number : numbers) {
      final double value = number.doubleValue();
      sum += value;
      squared_sum += value * value;
    }
    double mean = sum / numbers.size();
    double variance = squared_sum / numbers.size() - mean * mean;
    return 1 - Math.exp(-Math.sqrt(variance) / mean);
  }

  protected double computeUnevenness(final Iterable<RegionAssignment> assignments, final Function<RegionAssignment, ? extends Number> amount, final ImmutableSet<ServerName> allServers) {
    final Map<ServerName, Double> values = Maps.newHashMap();
    for (final RegionAssignment assignment : assignments) {
      Double oldValue = values.get(assignment.getNewServer());
      oldValue = oldValue == null ? 0 : oldValue;
      values.put(assignment.getNewServer(), oldValue + amount.apply(assignment).doubleValue());
    }

    for (final ServerName server : allServers) {
      if (!values.containsKey(server)) {
        values.put(server, 0d);
      }
    }

    for (final Map.Entry<ServerName, Double> entry : values.entrySet()) {
      values.put(entry.getKey(), entry.getValue() / getServerWeight(entry.getKey()));
    }
    return unevenness(values.values());
  }

  public String prettyPrint(final String label, final Iterable<RegionAssignment> assignments) {
    return String.format("  %-21s x %.2f -> %.3f", getClass().getSimpleName(), weight, weight * actuallyComputeCost(assignments));
  }

  protected final double getServerWeight(ServerName serverName) {
    return 1.0;
  }
}
