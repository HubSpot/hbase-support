package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableDataBalanceCost extends CostFunction {
  private Map<String, Double> tableAverageSizes;

  public TableDataBalanceCost(final double weight) {
    super(weight);
  }

  @Override
  public void setInitialData(final List<RegionStats> regions, ImmutableSet<ServerName> servers) {
    tableAverageSizes = Maps.newHashMap();
    for (final RegionStats region : regions) {
      final String table = region.getRegionInfo().getTableNameAsString();
      final double oldValue = tableAverageSizes.containsKey(table) ? tableAverageSizes.get(table) : 0.0;
      tableAverageSizes.put(table, oldValue + region.getSize());
      servers.add(region.getServerName());
    }
    for (final String table : tableAverageSizes.keySet()) {
      tableAverageSizes.put(table, tableAverageSizes.get(table) / servers.size());
    }
  }

  @Override
  public double getServerCost(ServerName serverName, final Collection<RegionStats> regions) {
    final Map<String, Double> cost = Maps.newHashMap();
    for (final RegionStats region : regions) {
      final String table = region.getRegionInfo().getTableNameAsString();
      final double oldValue = cost.containsKey(table) ? cost.get(table) : 0.0;
      cost.put(table, oldValue + region.getSize());
    }
    double totalCost = 0;
    for (final Map.Entry<String, Double> table : cost.entrySet()) {
      if (tableAverageSizes.get(table.getKey()) > 0) {
        totalCost += table.getValue() / tableAverageSizes.get(table.getKey());
      }
    }
    return totalCost / cost.size();
  }

  @Override
  public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
    Table<ServerName, String, Double> tableSizes = HashBasedTable.create();
    for (final RegionAssignment assignment : assignments) {
      final String tableName = assignment.getRegionStats().getRegionInfo().getTableNameAsString();
      Double oldValue = tableSizes.get(assignment.getNewServer(), tableName);
      oldValue = oldValue == null ? 0 : oldValue;
      tableSizes.put(assignment.getNewServer(), tableName, oldValue + assignment.getRegionStats().getSize());
    }
    double totalUnevenness = 0;
    for (final Map.Entry<String, Map<ServerName, Double>> entry : tableSizes.columnMap().entrySet()) {
      totalUnevenness += unevenness(entry.getValue().values());
    }
    return totalUnevenness / tableSizes.columnKeySet().size();
  }
}
