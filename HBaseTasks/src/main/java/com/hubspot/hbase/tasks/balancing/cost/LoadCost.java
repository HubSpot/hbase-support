package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Function;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;

public class LoadCost extends AbstractEvennessCost {
  public LoadCost(final double weight) {
    super(weight, new Function<RegionStats, Number>() {
      @Override
      public Number apply(final RegionStats input) {
        return input.getLoadWeight();
      }
    });
  }

  @Override
  public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    return computeUnevenness(assignments, new Function<RegionAssignment, Number>() {
      @Override
      public Number apply(final RegionAssignment input) {
        return input.getRegionStats().getLoadWeight();
      }
    }, getServers());
  }
}
