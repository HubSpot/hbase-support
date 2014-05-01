package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Function;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;

public class RegionCountCost extends AbstractEvennessCost {
  public RegionCountCost(final double weight) {
    super(weight, new Function<RegionStats, Number>() {
      @Override
      public Number apply(final RegionStats input) {
        return 1;
      }
    });
  }

  @Override
  public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    return computeUnevenness(assignments, new Function<RegionAssignment, Number>() {
      @Override
      public Number apply(final RegionAssignment input) {
        return 1;
      }
    }, getServers());
  }
}
