package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.common.base.Function;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.hbase.utils.Pair;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ProximateRegionKeyCost extends CostFunction {
  public ProximateRegionKeyCost(final double weight) {
    super(weight);
  }

  @Override
  protected double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    final Multimap<Pair<ServerName, String>, RegionAssignment> assignmentsByServerTable = Multimaps.index (assignments, new Function<RegionAssignment, Pair<ServerName, String>>() {
      @Nullable
      @Override
      public Pair<ServerName, String> apply(@Nullable final RegionAssignment input) {
        return input == null ? null : Pair.of(input.getNewServer(), input.getRegionStats().getRegionInfo().getTableNameAsString());
      }
    });

    double totalCost = 0;
    for (final Pair<ServerName, String> tableServerPair : assignmentsByServerTable.keySet()) {
      final List<RegionStats> regions = Lists.newArrayList(Iterables.transform(assignmentsByServerTable.get(tableServerPair), new Function<RegionAssignment, RegionStats>() {
        @Nullable
        @Override
        public RegionStats apply(@Nullable final RegionAssignment input) {
          return input == null ? null : input.getRegionStats();
        }
      }));
      totalCost += computeGaps(regions);
    }
		if (assignmentsByServerTable.isEmpty()) return 0;
    return Math.pow(totalCost / assignmentsByServerTable.size(), 0.3);
  }

  @Override
  public double getServerCost(ServerName serverName, final Collection<RegionStats> regions) {
    final Multimap<String, RegionStats> regionsByTable = Multimaps.index(regions, new Function<RegionStats, String>() {
      @Nullable
      @Override
      public String apply(@Nullable final RegionStats input) {
        return input == null ? null : input.getRegionInfo().getTableNameAsString();
      }
    });
    double totalCost = 0;
    for (final String table : regionsByTable.keySet()) {
      totalCost += computeGaps(regionsByTable.get(table));
    }
		if (regions.isEmpty()) return 0;
    return Math.pow(computeGaps(regions) / regions.size(), 0.3);
  }

  private double computeGaps(final Collection<RegionStats> regions) {
    final List<UnsignedLong> keys = Lists.newArrayListWithExpectedSize(regions.size() * 2);
    for (final RegionStats region : regions) {
      final byte[] startKey = region.getRegionInfo().getStartKey();
      final byte[] endKey = region.getRegionInfo().getEndKey();
      if (startKey.length == 0) {
        keys.add(UnsignedLong.ZERO);
      } else {
        keys.add(fromKey(startKey));
      }
      if (endKey.length == 0) {
        keys.add(UnsignedLong.MAX_VALUE);
      } else {
        keys.add(fromKey(endKey));
      }
    }
    Collections.sort(keys);
    if (keys.size() < 4) {
      return 0;
    }
    double totalCost = 0;
    for (int i = 2; i < keys.size(); ++i) {
      totalCost += 1.0 / (1 + Math.abs(keys.get(i).subtract(keys.get(i - 1)).longValue()));
    }
    return totalCost;
  }

  private UnsignedLong fromKey(final byte[] bytes) {
    if (bytes.length < Bytes.SIZEOF_LONG) {
      final byte[] newBytes = new byte[Bytes.SIZEOF_LONG];
      System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
      for (int i = bytes.length; i < Bytes.SIZEOF_LONG; ++i) {
        newBytes[i] = 0;
      }
      return UnsignedLong.asUnsigned(Bytes.toLong(newBytes));
    }
    return UnsignedLong.asUnsigned(Bytes.toLong(bytes));
  }

}
