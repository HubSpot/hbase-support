package com.hubspot.hbase.tasks.jobs;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.hubspot.hbase.tasks.config.commandargs.ForArg;
import com.hubspot.hbase.tasks.config.commandargs.HBaseTaskOption;
import com.hubspot.hbase.tasks.hdfs.HdfsLocalityInfo;
import com.hubspot.hbase.tasks.load.RegionLoadEstimation;
import com.hubspot.hbase.tasks.models.RegionStats;
import com.hubspot.hbase.tasks.serialization.ObjectMapperSingleton;
import com.hubspot.hbase.tasks.serialization.SerializationModule;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseAdminWrapper;
import org.apache.hadoop.hbase.ServerName;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class DisplayRegionStatsJob implements Runnable {
  public static final String SHORT_OPT = "displayRegionStats";
  public static final String LONG_OPT = "displayRegionStats";
  public static final String DESCRIPTION = "Get and show region stats for the cluster.";

  private final RegionLoadEstimation regionLoadEstimation;
  private final HBaseAdminWrapper hBaseAdminWrapper;
  private final HdfsLocalityInfo hdfsLocalityInfo;

  @Inject
  @ForArg(HBaseTaskOption.OUTPUT_FILE)
  private Optional<String> exportStatsFile;
  @Inject
  @ForArg(HBaseTaskOption.TABLE)
  private Optional<String> table;
  @Inject
  @ForArg(HBaseTaskOption.SERVER_NAME)
  private Optional<String> serverName;

  @Inject
  public DisplayRegionStatsJob(final RegionLoadEstimation regionLoadEstimation, final HBaseAdminWrapper hBaseAdminWrapper, final HdfsLocalityInfo hdfsLocalityInfo) {
    this.regionLoadEstimation = regionLoadEstimation;
    this.hBaseAdminWrapper = hBaseAdminWrapper;
    this.hdfsLocalityInfo = hdfsLocalityInfo;
  }

  @Override
  public void run() {
    try {
      doWork();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void doWork() throws Exception {
    Multimap<ServerName, RegionStats> regionInfos = RegionStats.regionInfoToStats(hBaseAdminWrapper.getRegionInfosByServer(true));
    regionLoadEstimation.annotateRegionsWithLoadInPlace(regionInfos);
    hdfsLocalityInfo.annotateRegionsWithLocalityInPlace(regionInfos);

    if (exportStatsFile.isPresent()) {
      final File exportStatsFileObj = new File(exportStatsFile.get());
      SerializationModule.fixJacksonModule();
      ObjectMapperSingleton.MAPPER.writeValue(exportStatsFileObj, Lists.newArrayList(regionInfos.values()));
    }

    printRegionInfo("Region info by table", "Table name", regionInfos.values(), getTableFunction());

    System.out.println("\n\n");

    printRegionInfo("Region info by server", "Region server", regionInfos.values(), getServerFunction());

    if (table.isPresent()) {
      printRegionInfo("Region info by region for table " + table.get(), "Region", Iterables.filter(regionInfos.values(), filterByTablePredicate(table.get())), getRegionNameFunction());
    }

    if (serverName.isPresent()) {
      printRegionInfo("Region info by region for server " + serverName.get(), "Region", Iterables.filter(regionInfos.values(), filterByServerNamePredicate(serverName.get())), getRegionsOnServerFunction());
    }
  }

  private Function<RegionStats, String> getTableFunction() {
    return new Function<RegionStats, String>() {
      @Override
      public String apply(final RegionStats input) {
        return input.getRegionInfo().getTableNameAsString();
      }
    };
  }

  private Function<RegionStats, String> getServerFunction() {
    return new Function<RegionStats, String>() {
      @Override
      public String apply(final RegionStats input) {
        return input.getServerName().getHostname();
      }
    };
  }

  private Function<RegionStats, String> getRegionNameFunction() {
    return new Function<RegionStats, String>() {

      @Override
      public String apply(RegionStats input) {
        return input.getRegionInfo().getEncodedName();
      }
    };
  }

  private Function<RegionStats, String> getRegionsOnServerFunction() {
    return new Function<RegionStats, String>() {

      @Override
      public String apply(RegionStats input) {
        return String.format("%s, %s", input.getRegionInfo().getTableNameAsString(), input.getRegionInfo().getEncodedName());
      }
    };
  }

  private Predicate<RegionStats> filterByTablePredicate(final String tableName) {
    return new Predicate<RegionStats>() {

      @Override
      public boolean apply(RegionStats input) {
        return input.getRegionInfo().getTableNameAsString().equalsIgnoreCase(tableName);
      }
    };
  }

  private Predicate<RegionStats> filterByServerNamePredicate(final String serverName) {
    return new Predicate<RegionStats>() {

      @Override
      public boolean apply(RegionStats input) {
        return input.getServerName().getHostname().equalsIgnoreCase(serverName);
      }
    };
  }

  private void printRegionInfo(final String title, final String keyName, final Iterable<RegionStats> regionStats, final Function<RegionStats, String> indexFunction) {
    final Multimap<String, RegionStats> regionStatsMultimap = Multimaps.index(regionStats, indexFunction);
    final String row = Strings.repeat("-", 99);
    System.out.println("+" + row + "+");
    System.out.println("|" + StringUtils.center(title, 99) + "|");
    System.out.println("+" + row + "+");
    System.out.println(String.format("|%-50s|%-7s|%-8s|%-7s|%-7s|%-7s|%-7s|", keyName, "Count", "Load", "Size", "50th", "95th", "Lcty"));
    System.out.println(String.format("|%-50s+%-7s+%-8s+%-7s+%-7s+%-7s+%-7s|",
            Strings.repeat("-", 50),
            Strings.repeat("-", 7),
            Strings.repeat("-", 8),
            Strings.repeat("-", 7),
            Strings.repeat("-", 7),
            Strings.repeat("-", 7),
            Strings.repeat("-", 7)));

    final List<String> keys = Lists.newArrayList(regionStatsMultimap.keySet());
    Collections.sort(keys);

    double totalLoad = 0;

    for (final RegionStats stats : regionStatsMultimap.values()) {
      totalLoad += stats.getLoadWeight();
    }

    for (final String key : keys) {
      final List<RegionStats> regionStatsList = Lists.newArrayList(regionStatsMultimap.get(key));
      Collections.sort(regionStatsList, RegionStats.getSizeComparator());
      long totalSize = 0;
      long transferCost = 0;
      for (final RegionStats stats : regionStatsList) {
        totalSize += stats.hasSize() ? stats.getSize() : 0L;
        transferCost += stats.transferCost(stats.getServerName());
      }
      double tableLoad = 0;
      if (totalLoad > 0) {
        for (final RegionStats region : regionStatsList) {
          tableLoad += region.getLoadWeight();
        }
        tableLoad /= totalLoad;
      }
      System.out.println(String.format("|%-50s|%-7s|%-8s|%-7s|%-7s|%-7s|%-7s|",
              key,
              regionStatsList.size(),
              String.format("%.3f", tableLoad),
              prettyByteDisplay(totalSize),
              prettyByteDisplay(regionStatsList.get(regionStatsList.size() >> 1).getSize()),
              prettyByteDisplay(regionStatsList.get((regionStatsList.size() * 95) / 100).getSize()),
              String.format("%.4f", totalSize == 0 ? 1.0 : 1 - (((double) transferCost) / totalSize)))
      );
    }

    System.out.println("+" + row + "+");
  }

  private String prettyByteDisplay(final long bytes) {
    return bytes < 2048 ? String.valueOf(bytes) : FileUtils.byteCountToDisplaySize(bytes);
  }
}
