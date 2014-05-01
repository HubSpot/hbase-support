package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Function;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;

public class LocalityCost extends AbstractEvennessCost {
  public LocalityCost(final double weight) {
    super(weight, new Function<RegionStats, Number>() {
      @Override
      public Number apply(final RegionStats input) {
        return input.getSize();
      }
    });
  }

  @Override
  public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    double totalSize = 0;
    double unlocalSize = 0;
    for (final RegionAssignment assignment : assignments) {
      totalSize += assignment.getRegionStats().getSize();
      unlocalSize += assignment.getRegionStats().transferCost(assignment.getNewServer());
    }
    return unlocalSize / totalSize;
  }
}
