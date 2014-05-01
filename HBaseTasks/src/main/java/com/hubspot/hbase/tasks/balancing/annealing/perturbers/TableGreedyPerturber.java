package com.hubspot.hbase.tasks.balancing.annealing.perturbers;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.hubspot.hbase.tasks.balancing.annealing.AssignmentConfig;
import com.hubspot.hbase.tasks.balancing.config.OptimizationModule;
import com.hubspot.hbase.tasks.balancing.cost.CostFunction;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.models.RegionStats;
import org.apache.hadoop.hbase.ServerName;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hubspot.hbase.tasks.balancing.annealing.Iterables3.pickRandom;

public class TableGreedyPerturber implements Perturber {
  private static final int stepsPerTable = 10;
  private final CostFunction costFunction;
  private AtomicInteger stepsSinceUpdate = new AtomicInteger(stepsPerTable);
  private String currentTable = null;
  private Set<String> seenTables = Sets.newHashSet();
  private Set<String> allTables;
  private final GreedyPerturber greedyPerturber;
  @Inject
  @ForArg(HBaseTaskOption.TABLE)
  Optional<String> table;


  @Inject
  public TableGreedyPerturber(@Named(OptimizationModule.COST_FUNCTION) final CostFunction costFunction,
                              final GreedyPerturber greedyPerturber) {
    this.costFunction = costFunction;
    this.greedyPerturber = greedyPerturber;
  }

  @Override
  public AssignmentConfig perturb(final AssignmentConfig initial, final double temp) {
    if (table.isPresent()) {
      return greedyPerturber.perturb(initial, temp);
    }
    if (allTables == null) {
      allTables = Sets.newHashSet(Iterables.transform(initial.getAllRegions(), RegionStats.REGION_STATS_TO_TABLE_NAME));
    }

    while (true) {
      final String table = getTable(initial, temp);

      final Map<ServerName, Double> serverCosts = getServerCosts(initial.getAssignmentsByServer(), table, initial.getServers());
      final List<Map.Entry<ServerName, Double>> orderedCosts = Lists.newArrayList(serverCosts.entrySet());

      Collections.sort(orderedCosts, new Comparator<Map.Entry<ServerName, Double>>() {
        @Override
        public int compare(final Map.Entry<ServerName, Double> serverNameDoubleEntry, final Map.Entry<ServerName, Double> serverNameDoubleEntry2) {
          return Doubles.compare(serverNameDoubleEntry2.getValue(), serverNameDoubleEntry.getValue());
        }
      });

      final Multimap<ServerName, RegionStats> assignments = initial.getAssignmentsByServer();

      final List<RegionStats> regionsForTableOnServer = Lists.newArrayList(Iterables.filter(assignments.get(orderedCosts.get(0).getKey()), isForTable(table)));

      if (regionsForTableOnServer.isEmpty()) continue;

      while (true) {
        Optional<AssignmentConfig> maybeConfig = initial.assign(
                pickRandom(regionsForTableOnServer),
                orderedCosts.get(orderedCosts.size() - 1).getKey()
        );
        if (maybeConfig.isPresent()) {
          return maybeConfig.get();
        }
      }
    }
  }

  private Predicate<RegionStats> isForTable(final String table) {
    return new Predicate<RegionStats>() {
      @Override
      public boolean apply(@Nullable final RegionStats input) {
        return input != null && table.equalsIgnoreCase(input.getRegionInfo().getTableNameAsString());
      }
    };
  }

  protected Map<ServerName, Double> getServerCosts(Multimap<ServerName, RegionStats> regions, final String tableName, final Set<ServerName> servers) {
    Map<ServerName, Double> result = Maps.newHashMap();

    for (final ServerName server : regions.keySet()) {
      final List<RegionStats> regionsForTable = ImmutableList.copyOf(Iterables.filter(regions.get(server),
              Predicates.compose(Predicates.equalTo(tableName), REGION_STATS_TO_TABLE)));
      result.put(server, costFunction.getServerCost(server, regionsForTable));
    }

    for (final ServerName server : servers) {
      if (!result.containsKey(server)) {
        result.put(server, 0d);
      }
    }
    return result;
  }

  private String getTable(final AssignmentConfig assignmentConfig, final double temp) {
    if (currentTable == null || stepsSinceUpdate.getAndAdd(1) >= stepsPerTable) {
      stepsSinceUpdate.set(0);

      final Map<String, Double> tableCosts = getTableCosts(assignmentConfig.getAllRegions());

      final List<Map.Entry<String, Double>> orderedCosts = Lists.newArrayList(tableCosts.entrySet());

      Collections.sort(orderedCosts, new Comparator<Map.Entry<String, Double>>() {
        @Override
        public int compare(final Map.Entry<String, Double> o1, final Map.Entry<String, Double> o2) {
          return Doubles.compare(o2.getValue(), o1.getValue());
        }
      });

      if (Sets.difference(allTables, seenTables).isEmpty()) {
        seenTables.clear();
      }

      currentTable = pickRandom(Iterables.limit(orderedCosts,
              Math.min(orderedCosts.size(), Math.max(2, (int) (temp * orderedCosts.size())))
      )).getKey();

      for (final Map.Entry<String, Double> tableCost : orderedCosts) {
        final String table = tableCost.getKey();
        if (!seenTables.contains(table)) {
          currentTable = table;
          seenTables.add(currentTable);
          break;
        }
      }
    }
    return currentTable;
  }

  private Map<String, Double> getTableCosts(Iterable<RegionStats> regions) {
    final Multimap<String, RegionStats> regionsByTable = Multimaps.index(regions, REGION_STATS_TO_TABLE);
    final Map<String, Double> result = Maps.newHashMap();

    for (final String tableName : regionsByTable.keySet()) {
      result.put(tableName, costFunction.getServerCost(null, regionsByTable.get(tableName)));
    }

    return result;
  }

  private static final Function<RegionStats, String> REGION_STATS_TO_TABLE = new Function<RegionStats, String>() {
    @Override
    public String apply(@Nullable final RegionStats input) {
      return input == null ? null : input.getRegionInfo().getTableNameAsString();
    }
  };

}
