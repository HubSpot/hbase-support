package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Function;
import com.hubspot.hbase.tasks.models.RegionStats;

public class DataSizeCost extends AbstractEvennessCost {
  public DataSizeCost(final double weight) {
    super(weight, new Function<RegionStats, Number>() {
      @Override
      public Number apply(final RegionStats input) {
        return input.getSize();
      }
    });
  }
}
