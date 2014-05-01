package com.hubspot.hbase.tasks.balancing.cost;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.hubspot.hbase.tasks.balancing.RegionAssignment;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import java.util.Collection;
import java.util.List;

public class CostFunctions {
  private CostFunctions() {
  }

  public static CostFunction combine(final CostFunction... functions) {
    return new CostFunction(0) {
      @Override
      public void setInitialData(final List<RegionStats> regions, ImmutableSet<ServerName> servers) {
        for (final CostFunction function : functions) {
          function.setInitialData(regions, servers);
        }
      }

      @Override
      public double actuallyComputeCost(final Iterable<RegionAssignment> assignments) {
        double totalCost = 0;
        for (final CostFunction function : functions) {
          totalCost += function.getWeight() * function.computeCost(assignments);
        }
        return totalCost;
      }

      @Override
      public double getServerCost(ServerName serverName, final Collection<RegionStats> regions) {
        double totalCost = 0;
        for (final CostFunction function : functions) {
          totalCost += function.getWeight() * function.getServerCost(serverName, regions);
        }
        return totalCost;
      }

      @Override
      public String prettyPrint(final String label, final Iterable<RegionAssignment> assignments) {
        final StringBuilder builder = new StringBuilder();
        final String row = Strings.repeat("-", 45);
        builder.append(String.format("%s\n  Cost for '%s'\n", row, label));
        builder.append(row + "\n");
        double totalCost = actuallyComputeCost(assignments);
        for (final CostFunction function : functions) {
          builder.append(String.format(" %s\n", function.prettyPrint(label, assignments)));
        }
        builder.append(String.format("%s\n     TOTAL: %.3f\n%s\n\n", row, totalCost, row));
        return builder.toString();
      }

      @Override
      public String toString() {
        final StringBuilder builder = new StringBuilder("CostFunction {");
        int i = 0;
        for (CostFunction function : functions) {
          if ((i++) > 0) builder.append(", ");
          builder.append(String.format("%s x %s", function.getClass().getSimpleName(), function.getWeight()));
        }
        return builder.append("}").toString();
      }
    };
  }
}
